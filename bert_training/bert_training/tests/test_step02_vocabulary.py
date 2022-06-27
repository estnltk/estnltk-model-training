import os
import shutil
import unittest
from os.path import isdir
from pathlib import Path

from transformers import BertTokenizerFast

from pipelines.step02_BERT_pre_training.tokenizing.vocabulary_creator import create_vocabulary


def load_test_file(path):
    text = ""
    with open(path, encoding="utf-8") as file:
        for i in file:
            text += i
    return text


def file_len(path):
    with open(path, encoding="utf-8") as f:
        for i, _ in enumerate(f):
            pass
    return i + 1


class textCleaningTestsCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)
    model_path = ROOT_DIR + "/data/test_model_step02_vocabulary"

    def setUp(self) -> None:
        input = [self.ROOT_DIR + "/data/tsv_res_clean_r_events_exp.tsv"]

        if not isdir(self.model_path):
            os.mkdir(self.model_path)

        size = 6000
        special_tokens = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]", "<INT>",
                          "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>", "<br>"]
        create_vocabulary(self.model_path, input, size, special_tokens=special_tokens)

    def tearDown(self) -> None:
        shutil.rmtree(self.model_path)

    def test_vocabulary_creation_unigram(self):
        actual = load_test_file(self.model_path + "/vocab.txt")

        # testing that the vocabulary contains ctrl symbols
        for i in "[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]":
            self.assertIn(i, actual)

    def test_tokenizer(self):

        additional_special_tokens = ["<INT>", "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>", "<br>"]
        tokenizer = BertTokenizerFast.from_pretrained(self.model_path,
                                                      do_lower_case=False,
                                                      additional_special_tokens=additional_special_tokens)

        # testing that encoding and decoding works and adds necessary tokens
        output = tokenizer.encode("<XXX> <FLOAT> , võrreldes eelmise visiidiga <DATE> .", )
        expected = "[CLS] <XXX> <FLOAT>, võrreldes eelmise visiidiga <DATE>. [SEP]"
        self.assertEqual(expected, tokenizer.decode(output))


if __name__ == '__main__':
    unittest.main()
