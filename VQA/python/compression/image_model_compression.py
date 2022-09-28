from __future__ import print_function, division, absolute_import
import argparse
import os
import numpy as np

import torch
import torch.nn.parallel
import torch.optim
import torch.utils.data
from torchvision.models.resnet import BasicBlock as ResNetBasicBlock

import torch_pruning as tp
from nni.algorithms.compression.pytorch.pruning import L1FilterPruner
from nni.compression.pytorch import ModelSpeedup

import sys
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(dir_path))
sys.path.append(os.path.join(os.path.dirname(dir_path), 'vqa/external/pretrained-models.pytorch'))

import pretrainedmodels
import vqa.models.convnets as convnets

model_names = sorted(name for name in pretrainedmodels.__dict__
                     if not name.startswith("__")
                     and name.islower()
                     and callable(pretrainedmodels.__dict__[name]))

class ImageModelCompressor:
    def __init__(self, config):
        arch = config["arch"]
        device = config["device"]

        device = str(device).strip().lower().replace('cuda:', '')  # to string, 'cuda:0' to '0'
        if device == 'cpu':
            self.device = torch.device('cpu')
        else:
            if device.isdecimal():
                self.device = torch.device('cuda:{:d}'.format(int(device)))
            else:
                self.device = torch.device('cuda:0')
    
        opt_factory_cnn = {
            'arch': arch
        }
        self.arch = arch
        self.model = convnets.factory(opt_factory_cnn)
        self.model = self.model.to(self.device)
        self.model.eval()

    @staticmethod
    def prune_conv(DG, conv, amount=0.2):
        strategy = tp.strategy.L1Strategy()
        pruning_index = strategy(conv.weight, amount=amount)
        plan = DG.get_pruning_plan(conv, tp.prune_conv, pruning_index)
        plan.exec()
    
    def channel_pruning(self, tool='torch-pruning', img_size=448):
        dir_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        dir_path = os.path.join(dir_path, "comp_models")
        os.system('mkdir -p ' + dir_path)
        
        rand_input_cpu = torch.rand(1, 3, img_size, img_size)
        rand_input = rand_input_cpu.to(self.device)
        output = self.model(rand_input)
        print("orig: input: {} => output: {}".format(rand_input.size(), output.size()))
        params = sum([np.prod(p.size()) for p in self.model.parameters()])
        print("orig model #parameters: %.1fM"%(params / 1e6))

        if tool == 'torch-pruning':
            save_path = os.path.join(dir_path, "{}_channel_pruned.pth".format(self.arch))
            DG = tp.DependencyGraph().build_dependency(self.model, rand_input_cpu)
            block_prune_probs = [0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.3, 0.3]
            blk_id = 0
            for m in self.model.modules():
                if isinstance(m, ResNetBasicBlock):
                    self.prune_conv(DG, m.conv1, block_prune_probs[blk_id])
                    self.prune_conv(DG, m.conv2, block_prune_probs[blk_id])
                    blk_id += 1
            print("saving pruned model...")
            torch.save(self.model, save_path)             
            # test
            print("loading pruned model...")            
            self.model = torch.load(save_path).to(self.device)
            params = sum([np.prod(p.size()) for p in self.model.parameters()])
            print("pruned model #parameters: %.1fM"%(params / 1e6))            
            output = self.model(rand_input)
            print("pruned: input: {} => output: {}".format(rand_input.size(), output.size()))        

            rand_input_half = torch.rand(1, 3, img_size // 2, img_size // 2).to(self.device)
            output = self.model(rand_input_half)
            print("pruned: input: {} => output: {}".format(rand_input_half.size(), output.size()))                

        elif tool == 'nni':
            config_list = [{ 'sparsity': 0.8, 'op_types': ['Conv2d'] }]
            pruner = L1FilterPruner(self.model, config_list)
            pruner.compress()
            model_save_path = os.path.join(dir_path, "{}_channel_pruned_model.pth".format(self.arch)) 
            mask_save_path = os.path.join(dir_path, "{}_channel_pruned_mask.pth".format(self.arch)) 
            pruner.export_model(model_path=model_save_path, mask_path=mask_save_path)
            
            # test
            # self.model = torch.load_state_dict(model_save_path)
            params = sum([np.prod(p.size()) for p in self.model.parameters()])
            print("pruned model #parameters w/o NNI speedup: %.1fM"%(params / 1e6))      
            m_speedup = ModelSpeedup(self.model, rand_input, mask_save_path)
            m_speedup.speedup_model()
            print("pruned model #parameters with NNI speedup: %.1fM"%(params / 1e6))      
            output = self.model(rand_input)
            print("pruned: input: {} => output: {}".format(rand_input.size(), output.size()))                    

            rand_input_half = torch.rand(1, 3, img_size // 2, img_size // 2).to(self.device)
            output = self.model(rand_input_half)
            print("pruned: input: {} => output: {}".format(rand_input_half.size(), output.size()))                    
        
        else:
            raise ValueError("invalid pruning tool")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Image Model Compressor")
    parser.add_argument('--arch', '-a', metavar='ARCH', default='nasnetamobile',
                    choices=model_names,
                    help='model architecture: ' +
                         ' | '.join(model_names) +
                         ' (default: nasnetamobile)')
    parser.add_argument('--device', '-d', type=int, default=0, help='which device to use')
    parser.add_argument('--tool', '-t', type=str, default='nni', help='which pruning tool to use')

    args = parser.parse_args()

    config = {
        "arch": args.arch,
        "device": args.device
    }

    compressor = ImageModelCompressor(config)
    compressor.channel_pruning(tool=args.tool)