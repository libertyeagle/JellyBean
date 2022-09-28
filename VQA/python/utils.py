import os
import cv2
import librosa
import numpy as np


def load_images(paths):
    images = []
    if type(paths) is not list:
        paths = [paths]
    for path in paths:
        if not os.path.isfile(path):
            raise ValueError("file does not exist!")
        image = cv2.imread(path, flags=cv2.IMREAD_COLOR)
        images.append(image)
    if len(images) > 1:
        images = np.stack(images, axis=0)
        return images
    else:
        return images[0]

def read_audio_files(paths, sampling_rate):
    raw_speech = []
    if type(paths) is not list:
        paths = [paths]
    for path in paths:
        if not os.path.isfile(path):
            raise ValueError("File does not exist!")
        raw_speech.append(librosa.load(path, sr=sampling_rate, mono=True)[0]) 
    if len(raw_speech) > 1:
        return raw_speech
    else:
        return raw_speech[0]    