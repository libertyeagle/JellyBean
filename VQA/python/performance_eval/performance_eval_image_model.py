from __future__ import print_function, division, absolute_import
import argparse
import os
import time

import torch
import torch.nn.parallel
import torch.optim
import torch.utils.data
import torchvision.datasets as datasets

import sys
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(os.path.dirname(dir_path), 'vqa/external/pretrained-models.pytorch'))

import pretrainedmodels
import pretrainedmodels.utils

model_names = sorted(name for name in pretrainedmodels.__dict__
                     if not name.startswith("__")
                     and name.islower()
                     and callable(pretrainedmodels.__dict__[name]))


class ImageModelPerformanceEvaluator:
    def __init__(self, config):
        arch = config["model-arch"]
        compression = config.get("model-compression", None)
        device = config["device"]
        ckpt = config.get("resume_ckpt", None) # optional checkpoint to resume

        device = str(device).strip().lower().replace('cuda:', '')  # to string, 'cuda:0' to '0'
        if device == 'cpu':
            self.device = torch.device('cpu')
        else:
            if device.isdecimal():
                self.device = torch.device('cuda:{:d}'.format(int(device)))
            else:
                self.device = torch.device('cuda:0') 
        
        if compression is None:
            print("=> creating model '{}'".format(arch))                
            self.model = pretrainedmodels.__dict__[arch](num_classes=1000,
                                                         pretrained="imagenet")
        
        if ckpt:
            if os.path.isfile(ckpt):
                print("=> loading checkpoint '{}'".format(ckpt))
                checkpoint = torch.load(ckpt)
                self.model.load_state_dict(checkpoint['state_dict'])
                print("=> loaded checkpoint '{}' (epoch {})"
                    .format(ckpt, checkpoint['epoch']))
            else:
                print("=> no checkpoint found at '{}'".format(ckpt))

        self.arch = arch
        self.model = self.model.to(self.device)
        self.model.eval()

    def evaluate_performance(self, batch_size=64):
        dir_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        val_dir = os.path.join(dir_path, "data/imagenet/val")

        if 'scale' in pretrainedmodels.pretrained_settings[self.arch]["imagenet"]:
            scale = pretrainedmodels.pretrained_settings[self.arch]["imagenet"]['scale']
        else:
            scale = 0.875        

        print('Images transformed from size {} to {}'.format(
            int(round(max(self.model.input_size) / scale)),
            self.model.input_size))

        val_tf = pretrainedmodels.utils.TransformImage(
            self.model,
            scale=scale,
            preserve_aspect_ratio=True
        )
        val_loader = torch.utils.data.DataLoader(
            datasets.ImageFolder(val_dir, val_tf),
            batch_size=batch_size, shuffle=False,
            num_workers=4, pin_memory=True)

        with torch.no_grad():
            batch_time = AverageMeter()
            top1 = AverageMeter()
            top5 = AverageMeter()

            end = time.time()
            total_time = 0.0
            total_samples = 0
            for i, (input, target) in enumerate(val_loader):
                target = target.to(self.device)
                input = input.to(self.device)

                start_event = torch.cuda.Event(enable_timing=True)
                end_event = torch.cuda.Event(enable_timing=True)
                # compute output
                start_event.record()
                output = self.model(input)
                end_event.record()
                torch.cuda.synchronize()
                total_time += start_event.elapsed_time(end_event)
                total_samples += input.size(0)

                # measure accuracy and record loss
                prec1, prec5 = accuracy(output.data, target.data, topk=(1, 5))
                top1.update(prec1.item(), input.size(0))
                top5.update(prec5.item(), input.size(0))

                # measure elapsed time
                batch_time.update(time.time() - end)
                end = time.time()

                if i % 10 == 0:
                    print('Test: [{0}/{1}]\t'
                        'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
                        'Acc@1 {top1.val:.3f} ({top1.avg:.3f})\t'
                        'Acc@5 {top5.val:.3f} ({top5.avg:.3f})'.format(
                        i, len(val_loader), batch_time=batch_time,
                        top1=top1, top5=top5))

        return top1.avg, top5.avg, total_time / total_samples

class AverageMeter(object):
    """Computes and stores the average and current value"""
    def __init__(self):
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count


def accuracy(output, target, topk=(1,)):
    """Computes the precision@k for the specified values of k"""
    maxk = max(topk)
    batch_size = target.size(0)

    _, pred = output.topk(maxk, 1, True, True)
    pred = pred.t()
    correct = pred.eq(target.reshape(1, -1).expand_as(pred))

    res = []
    for k in topk:
        correct_k = correct[:k].reshape(-1).float().sum(0)
        res.append(correct_k.mul_(100.0 / batch_size))
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Image Model Performance Evaluation")
    parser.add_argument('--arch', '-a', metavar='ARCH', default='nasnetamobile',
                    choices=model_names,
                    help='model architecture: ' +
                         ' | '.join(model_names) +
                         ' (default: nasnetamobile)')
    parser.add_argument('--compression', '-c', type=str, default=None, help='model compression')
    parser.add_argument('--batch_size', '-b', type=int, default=8, help='batch size')
    parser.add_argument('--device', '-d', type=int, default=0, help='which device to use')
    parser.add_argument('--log_file', '-f', type=str, help='where to save the results')

    args = parser.parse_args()

    config = {
        "model-arch": args.arch,
        "model-compression": args.compression,
        "device": args.device
    }

    engine = ImageModelPerformanceEvaluator(config)
    top1_acc, top5_acc, avg_time = engine.evaluate_performance(batch_size=args.batch_size)
    print("Acc@1={:.4f}, ACC@5={:.4f}, avg_time/sample={:.6f} sec".format(top1_acc, top5_acc, avg_time / 1000))
    if args.log_file:
        with open(args.log_file, 'at') as f:
            print("arch={}, Acc@1={:.4f}, ACC@5={:.4f}, avg_time/sample={:.6f} sec".format(args.arch, top1_acc, top5_acc, avg_time / 1000), file=f)            
    else:
            print("arch={}, Acc@1={:.4f}, ACC@5={:.4f}, avg_time/sample={:.6f} sec".format(args.arch, top1_acc, top5_acc, avg_time / 1000))            
