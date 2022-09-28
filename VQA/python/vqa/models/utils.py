import sys
import copy
import torch
import torch.nn as nn
import torchvision.models as models

from .noatt import MLBNoAtt, MutanNoAtt
from .att import MLBAtt, MutanAtt

model_names = sorted(name for name in sys.modules[__name__].__dict__
    if not name.startswith("__"))# and 'Att' in name)

def factory(opt, vocab_words, vocab_answers):
    opt = copy.copy(opt)

    if opt['arch'] in model_names:
        model = getattr(sys.modules[__name__], opt['arch'])(opt, vocab_words, vocab_answers)
    else:
        raise ValueError

    return model