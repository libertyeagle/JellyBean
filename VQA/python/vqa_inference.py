import os
import yaml

import torch
from torch.nn.utils.rnn import pad_sequence

import vqa.lib.utils as utils
import vqa.datasets as datasets
import vqa.models as models
from vqa.datasets.vqa_processed import tokenize_mcb
from train import load_checkpoint

class VQAInferenceEngine:
    def __init__(self, config):
        """Construct VQA Inference Engine
        Parameters
        ----------
        config: dict
            * config_path: str, path to yaml config file
            * device: str, which device to use
            * ckpt_path: str, path to checkpoints
            * resume_ckpt: str, which checkpoint to resume
        Returns
        -------
        VQAInferenceEngine
        """
        yaml_path = config["config_path"]
        ckpt_path = config["ckpt_path"]
        resume_ckpt = config["resume_ckpt"]
        device = config["device"]

        device = str(device).strip().lower().replace('cuda:', '')  # to string, 'cuda:0' to '0'
        if device == 'cpu':
            self.device = torch.device('cpu')
        else:
            if device.isdecimal():
                self.device = torch.device('cuda:{:d}'.format(int(device)))
            else:
                self.device = torch.device('cuda:0')

        options = {
            'logs': {
                'dir_logs': ckpt_path
            },
        }    
        with open(yaml_path, 'r') as handle:
            options_yaml = yaml.safe_load(handle)
        options = utils.update_values(options, options_yaml)

        self.max_question_len = options['vqa']['maxlength']
        self.att_feat = not "NoAtt" in options['model']['arch']

        trainset = datasets.factory_VQA(options['vqa']['trainsplit'], options['vqa'])
        self.word_to_wid = trainset.word_to_wid
        self.aid_to_ans = trainset.aid_to_ans

        self.vqa_model = models.factory(options['model'],
                            trainset.vocab_words(),
                            trainset.vocab_answers())
        self.vqa_model = self.vqa_model.to(self.device)
        self.vqa_model.eval()
        load_checkpoint(self.vqa_model, None,
            os.path.join(options['logs']['dir_logs'], resume_ckpt))

    def generate_answers_top_k(self, image_features, questions, topk=5):
        """
        Parmeters
        ---------
        image_features: ndarray
            Extracted image features
            (batch_size, channels, height, width) for att feat and batched input
            (channels, height, width) for att_feat and single image input
            (batch_size, feat_dim) for no_att feat and batched input
            (feat_dim, ) for no_att_feat and single image input            
        questions: str or list
            a list of questions (corresponding to the batch of images)
            or a single question in str
        topk: int
            generate topk answers for each question
        Returns
        -------
        list
            list of top-k answers for a single image-question pair
            or a list of list for batched input
        """
        # image_features and questions are ndarray
        # questions are right zero-padded
        image_features = torch.from_numpy(image_features).to(self.device)
        image_features = image_features.unsqueeze(0)
        questions = self.process_questions(questions)
        questions = questions.to(self.device)
        with torch.no_grad():
            answers_scores = self.vqa_model(image_features, questions)
            answers_scores = torch.nn.functional.softmax(answers_scores, dim=-1)
            # answers_ids: (batch_size, topk)
            _, answers_ids = torch.topk(answers_scores, k=topk, dim=1, largest=True, sorted=True)
            answers_ids = answers_ids.cpu().numpy()

        batch_answers = []
        for i in range(answers_ids.shape[0]):
            answers = []
            for k in range(topk):
                answer = self.aid_to_ans[answers_ids[i][k]]
                answers.append(answer)
            batch_answers.append(answers)
        if len(batch_answers) == 1:
            batch_answers = batch_answers[0]

        return batch_answers


    def generate_answer(self, image_features, questions):
        """
        Parmeters
        ---------
        image_features: ndarray
            Extracted image features
            (batch_size, channels, height, width) for att feat and batched input
            (channels, height, width) for att_feat and single image input
            (batch_size, feat_dim) for no_att feat and batched input
            (channels, feat_dim) for no_att_feat and single image input            
        questions: str or list
            a list of questions (corresponding to the batch of images)
            or a single question in str
        Returns
        -------
        list or str
            answer for a single image-question pair (in str)
            or list of answers for batched input
        """        
        # image_features and questions are ndarray
        # questions are right zero-padded
        image_features = torch.from_numpy(image_features).to(self.device)
        image_features = image_features.unsqueeze(0)
        questions = self.process_questions(questions)
        questions = questions.to(self.device)
        with torch.no_grad():
            answers_scores = self.vqa_model(image_features, questions)
            answers_scores = torch.nn.functional.softmax(answers_scores, dim=-1)
            # answers_ids: (batch_size, )
            answers_ids = torch.argmax(answers_scores, dim=1)
            answers_ids = answers_ids.cpu().numpy()

        batch_answers = [self.aid_to_ans[aid] for aid in answers_ids]
        if len(batch_answers) == 1:
            batch_answers = batch_answers[0]

        return batch_answers

    def process_questions(self, questions):
        if type(questions) is not list:
            questions = [questions]
        with torch.no_grad():
            questions_wids = []
            for question_str in questions:
                question_tokens = tokenize_mcb(question_str)
                question_len = min(len(question_tokens), self.max_question_len)
                question_data = torch.zeros(question_len, dtype=torch.long)
                for i, word in enumerate(question_tokens[:question_len]):
                    if word in self.word_to_wid:
                        question_data[i] = self.word_to_wid[word]
                    else:
                        question_data[i] = self.word_to_wid['UNK']
                questions_wids.append(question_data)
            questions_wids = pad_sequence(questions_wids, batch_first=True)
        return questions_wids
