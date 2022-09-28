import copy
import torch
import torch.nn as nn
import torchvision.models as pytorch_models
import sys
import os
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(os.path.dirname(dir_path), 'external/pretrained-models.pytorch'))
import pretrainedmodels as torch7_models

pytorch_resnet_names = sorted(name for name in pytorch_models.__dict__
    if name.islower()
    and name.startswith("resnet")
    and callable(pytorch_models.__dict__[name]))

torch7_resnet_names = sorted(name for name in torch7_models.__dict__
    if name.islower()
    and callable(torch7_models.__dict__[name]))

model_names = pytorch_resnet_names + torch7_resnet_names

class WrapperModule(nn.Module):
    def __init__(self, net, forward_fn):
        super(WrapperModule, self).__init__()
        self.net = net
        self.forward_fn = forward_fn

    def forward(self, x):
        return self.forward_fn(self.net, x)

    def __getattr__(self, attr):
        try:
            return super(WrapperModule, self).__getattr__(attr)
        except AttributeError:
            return getattr(self.net, attr)


def forward_resnet(self, x):
    x = self.conv1(x)
    x = self.bn1(x)
    x = self.relu(x)
    x = self.maxpool(x)
    x = self.layer1(x)
    x = self.layer2(x)
    x = self.layer3(x)
    x = self.layer4(x)

    return x


def forward_resnext(self, x):
    x = self.features(x)

    return x
    

def factory(opt):
    opt = copy.copy(opt)
    pooling = 'pooling' in opt and opt['pooling']
    assert(not pooling, "pooling should not be set in opt")

    if opt['arch'] in pytorch_resnet_names:
        model = pytorch_models.__dict__[opt['arch']](pretrained=True)

        model = WrapperModule(model, forward_resnet) # ugly hack in case of DataParallel wrapping

    elif opt['arch'] == 'fbresnet152':
        model = torch7_models.__dict__[opt['arch']](num_classes=1000,
                                                    pretrained='imagenet')

        model = WrapperModule(model, forward_resnet) # ugly hack in case of DataParallel wrapping

    elif opt['arch'] in torch7_resnet_names:
        model = torch7_models.__dict__[opt['arch']](num_classes=1000,
                                                    pretrained='imagenet')

        model = WrapperModule(model, forward_resnext) # ugly hack in case of DataParallel wrapping

    else:
        raise ValueError

    return model
