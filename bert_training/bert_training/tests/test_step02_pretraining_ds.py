import os
import shutil
import unittest

from os.path import isdir
from pathlib import Path

from datasets import load_dataset
from transformers import BertTokenizerFast

from pipelines.step02_BERT_pre_training.tokenizing.text_dataset_for_NSP import create_dataset_for_NSP
from pipelines.step02_BERT_pre_training.tokenizing.vocabulary_creator import create_vocabulary


class PretrainingDatasetCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)
    model_path = ROOT_DIR + "/data/test_model_step01_pretraining_ds"
    train_files = [ROOT_DIR + "/data/corp_res_clean_r_events_par.tsv"]

    def setUp(self):

        # if dir already exists, then delete it
        if isdir(self.model_path):
            shutil.rmtree(self.model_path)

        # creating a new vocabulary
        os.mkdir(self.model_path)
        size = 6000
        special_tokens = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]", "<INT>",
                          "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>", "<br>"]
        create_vocabulary(self.model_path, self.train_files, size, special_tokens=special_tokens)

    def tearDown(self):
        shutil.rmtree(self.model_path)

    def test_NSP_dataset_creation(self):
        additional_special_tokens = ["<INT>", "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>", "<br>"]

        # loading the tokenizer
        tokenizer = BertTokenizerFast.from_pretrained(self.model_path,
                                                      do_lower_case=False,
                                                      additional_special_tokens=additional_special_tokens)

        # loading the dataset
        dataset = load_dataset("csv", data_files={'train': self.train_files})['train']
        dataset_enc = create_dataset_for_NSP(dataset, tokenizer, nsp_probability=0.5)

        # testing that the number of sentence pairs is correct
        sentences = 0
        for doc in dataset["text"]:
            sentences += len(doc.split("\n")) - 1

        self.assertEqual(len(dataset_enc), sentences)


if __name__ == '__main__':
    unittest.main()
