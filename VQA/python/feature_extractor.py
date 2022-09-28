import numpy as np
import torch
import torchvision.transforms as transforms
import vqa.models.convnets as convnets

class ImageFeatureExtractor():
    def __init__(self, config):
        """Construct an image featuare extractor
        Parameters
        ----------
        config: dict
            * model: str, which model to use (model ckpt will be automatically download)
            * device: str, which device to use
            * img_size: int, image size for inference (img_size, img_size)
            * hub_dir: str, where to store downloaded pretrained model
            * use_att_feat: bool, whether to use flatten feature or 3-dim attention feature
        Returns
        -------
        ImageFeatureExtractor
        """
        normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])
        # image transform pipelines                                                                        
        self.transform = transforms.Compose([
            transforms.ToPILImage(),
            transforms.Scale(config["img_size"]),
            transforms.CenterCrop(config["img_size"]),
            transforms.ToTensor(),
            normalize,
        ])

        hub_dir = config["hub_dir"]
        if hub_dir:
            torch.hub.set_dir(hub_dir)

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
            'arch': config["model"],
        }
        self.att_feat = config["use_att_feat"]
        self.image_model = convnets.factory(opt_factory_cnn)
        self.image_model = self.image_model.to(self.device)
        self.image_model.eval()


    def extract_features(self, images):
        """Extract image features
        Parameters
        ----------
        images: ndarray
            4-dim ndarray or 3-dim ndarray
            A single image in BGR format (height, width, channels) 
            or a batch of images (batch_size, height, width, channels)
        Returns
        -------
        ndarray
            (batch_size, channels, height, width) for att feat and batched input
            (channels, height, width) for att_feat and single image input
            (batch_size, feat_dim) for no_att feat and batched input
            (channels, feat_dim) for no_att_feat and single image input            
        """
        # images: ndarray in BGR format
        with torch.no_grad():
            tensor_images = []
            if images.ndim == 4:
                images = np.ascontiguousarray(images[:, :, :, ::-1])
                num_images = images.shape[0]
                for i in range(num_images):
                    image = self.transform(images[i])
                    tensor_images.append(image)
            else:
                images = np.ascontiguousarray(images[:, :, ::-1])
                tensor_images.append(self.transform(images))

            tensor_images = torch.stack(tensor_images, dim=0)
            tensor_images = tensor_images.to(self.device)
            visual_features = self.image_model(tensor_images)
            
            if not self.att_feat:
                nb_regions = visual_features.size(2) * visual_features.size(3)
                visual_features = visual_features.sum(3).sum(2).div(nb_regions).view(-1, visual_features.size(1))

            visual_features = visual_features.squeeze(0)
            # (batch_size, 2048, 14, 14) or (batch_size, 2048)
            visual_features = visual_features.cpu().numpy()

        return visual_features
