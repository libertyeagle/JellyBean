import os
import json


def read_dataset(dataset_path, num_instances):
    question_json_path = os.path.join(dataset_path, "MultipleChoice_mscoco_val2014_questions.json")
    questions = json.load(open(question_json_path, 'rt'))["questions"]

    image_dir = os.path.join(dataset_path, "coco_images/val2014")
    audio_dir = os.path.join(dataset_path, "questions_speech/val2014")

    image_paths = []
    audio_paths = []
    for question in questions:
        image_id = question["image_id"]
        question_id = question["question_id"]
        image_path = os.path.join(image_dir, "COCO_val2014_{:012d}.jpg".format(image_id))
        audio_path = os.path.join(audio_dir, "{}.flac".format(question_id))
        image_paths.append((question_id, image_path))
        audio_paths.append((question_id, audio_path))
    
    image_paths.sort(key=lambda x: x[0])
    audio_paths.sort(key=lambda x: x[0])

    image_paths = image_paths[:num_instances]
    audio_paths = audio_paths[:num_instances]
    image_paths = list(map(lambda x: (x[0], x[1][1]), enumerate(image_paths)))
    audio_paths = list(map(lambda x: (x[0], x[1][1]), enumerate(audio_paths)))

    return image_paths, audio_paths
    