import os
import shutil
import unittest
import numpy as np
from os.path import isdir, exists
from pathlib import Path

from datasets import load_dataset
from transformers import BertTokenizer, BertTokenizerFast

from pipelines.step01_create_training_corpus.textprocessing.pretraining_dataset import PreTrainingDataset
from pipelines.step02_BERT_pre_training.tokenizing.text_dataset_for_NSP import create_dataset_for_NSP
from pipelines.step02_BERT_pre_training.tokenizing.vocabulary_creator import create_vocabulary


class PretrainingDatasetCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def create_test_vocab(self, model_path, train_files):
        # if dir already exists, then delete it
        if isdir(model_path):
            shutil.rmtree(model_path)

        # creating a new vocabulary
        os.mkdir(model_path)
        size = 6000
        special_tokens = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]", "<INT>",
                          "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>", "<br>"]
        create_vocabulary(model_path, train_files, size, special_tokens=special_tokens)

    def test_NSP_dataset_creation(self):
        additional_special_tokens = ["<INT>", "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>", "<br>"]

        model_path = self.ROOT_DIR + "/data/test_model_tok_12341234124"
        input = [self.ROOT_DIR + "/data/corp_res_clean_r_events_par.tsv"]
        self.create_test_vocab(model_path, input)

        # loading the tokenizer
        tokenizer = BertTokenizerFast.from_pretrained(model_path,
                                                      do_lower_case=False,
                                                      additional_special_tokens=additional_special_tokens)

        # loading the dataset
        dataset = load_dataset("csv", data_files={'train': input})['train']
        dataset_enc = create_dataset_for_NSP(dataset, tokenizer, nsp_probability=0.5)

        # testing that the number of sentence pairs is correct
        sentences = 0
        for doc in dataset["text"]:
            sentences += len(doc.split("\n"))-1

        self.assertEqual(len(dataset_enc), sentences)



if __name__ == '__main__':
    unittest.main()
