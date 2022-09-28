import argparse
import os
import sys
import yaml
import json

import torch

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(dir_path))
import vqa.lib.engine as engine
import vqa.lib.utils as utils
import vqa.lib.logger as logger
import vqa.lib.criterions as criterions
import vqa.datasets as datasets
import vqa.models as models

parser = argparse.ArgumentParser(
    description='Train/Evaluate models',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
##################################################
# yaml options file contains all default choices #
parser.add_argument('--path_opt', default='options/vqa/default.yaml', type=str, 
                    help='path to a yaml options file')
################################################
# change cli options to modify default choices #
# logs options
parser.add_argument('-b', '--batch_size', type=int,
                    help='mini-batch size')
parser.add_argument('-j', '--workers', default=2, type=int,
                    help='number of data loading workers')
################################################


def main():
    global args 
    args = parser.parse_args()

    #########################################################################################
    # Create options
    #########################################################################################

    options = {
        'vqa' : {
            'trainsplit': "train"
        },
        'optim': {
            'batch_size': args.batch_size,
        }
    }
    if args.path_opt is not None:
        with open(args.path_opt, 'r') as handle:
            options_yaml = yaml.load(handle, Loader=yaml.SafeLoader)
        options = utils.update_values(options, options_yaml)

    #########################################################################################
    # Create needed datasets
    #########################################################################################

    trainset = datasets.factory_VQA(options['vqa']['trainsplit'],
                                    options['vqa'],
                                    options['coco'],
                                    None)                             

    valset = datasets.factory_VQA('val', options['vqa'], options['coco'])
    val_loader = valset.data_loader(batch_size=options['optim']['batch_size'],
                                    num_workers=args.workers)
    
    #########################################################################################
    # Create model, criterion and optimizer
    #########################################################################################
    
    model = models.factory(options['model'],
                           trainset.vocab_words(), trainset.vocab_answers(),
                           cuda=True, data_parallel=True)
    criterion = criterions.factory(options['vqa'], cuda=True)
    #########################################################################################
    # args.resume: resume from a checkpoint OR create logs directory
    #########################################################################################

    exp_logger = None
    args.start_epoch, _, exp_logger = load_checkpoint(model.module, 
        os.path.join(options['logs']['dir_logs'], "best"))

    #########################################################################################
    # args.evaluate: on valset OR/AND on testset
    #########################################################################################

    path_logger_json = os.path.join(options['logs']['dir_logs'], 'logger.json')

    _, val_results = engine.validate(val_loader, model, criterion,
                                            exp_logger, args.start_epoch, 10)
        
    # save results and compute OpenEnd accuracy
    exp_logger.to_json(path_logger_json)
    save_results(val_results, valset.split_name(),
                 options['logs']['dir_logs'], options['vqa']['dir'])
 


def make_meters():  
    meters_dict = {
        'loss': logger.AvgMeter(),
        'acc1': logger.AvgMeter(),
        'acc5': logger.AvgMeter(),
        'batch_time': logger.AvgMeter(),
        'data_time': logger.AvgMeter(),
        'epoch_time': logger.SumMeter()
    }
    return meters_dict

def save_results(results, split_name, dir_logs, dir_vqa):
    dir_epoch = os.path.join(dir_logs, 'results_text')
    name_json = 'OpenEnded_mscoco_{}_model_results.json'.format(split_name)
    # TODO: simplify formating
    if 'test' in split_name:
        name_json = 'vqa_' + name_json
    path_rslt = os.path.join(dir_epoch, name_json)
    os.system('mkdir -p ' + dir_epoch)
    with open(path_rslt, 'w') as handle:
        json.dump(results, handle)
    if not 'test' in split_name:
        os.system('python2 eval_res.py --dir_vqa {} --dir_epoch {} --subtype {} &'
                  .format(dir_vqa, dir_epoch, split_name))

def load_checkpoint(model, path_ckpt):
    path_ckpt_info  = path_ckpt + '_info.pth.tar'
    path_ckpt_model = path_ckpt + '_model.pth.tar'
    if os.path.isfile(path_ckpt_info):
        info = torch.load(path_ckpt_info)
        start_epoch = 0
        best_acc1   = 0
        exp_logger  = None
        if 'epoch' in info:
            start_epoch = info['epoch']
        else:
            print('no epoch to resume')
        if 'best_acc1' in info:
            best_acc1 = info['best_acc1']
        else:
            print('no best_acc1 to resume')
        if 'exp_logger' in info:
            exp_logger = info['exp_logger']
        else:
            print('no exp_logger to resume')
    else:
        print("no info checkpoint found at '{}'".format(path_ckpt_info))
    if os.path.isfile(path_ckpt_model):
        model_state = torch.load(path_ckpt_model)
        model.load_state_dict(model_state)
    else:
        print("no model checkpoint found at '{}'".format(path_ckpt_model))
    print("=> loaded checkpoint '{}' (epoch {}, best_acc1 {})"
              .format(path_ckpt, start_epoch, best_acc1))
    return start_epoch, best_acc1, exp_logger

if __name__ == '__main__':
    main()
