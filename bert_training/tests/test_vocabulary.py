import unittest

from tokenizers import Tokenizer
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

    def test_vocabulary_creation_unigram(self):
        model_path = "C:/Users/Meelis/PycharmProjects/medbert/data/test_model"
        input = ["C:/Users/Meelis/PycharmProjects/medbert/data/tsv_res_clean_r_events_exp.tsv"]
        size = 6000
        special_tokens = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]", "<INT>",
                          "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>"]
        create_vocabulary(model_path, input, size, special_tokens=special_tokens)
        actual = load_test_file(model_path + "/vocab.txt")

        # testing that the vocabulary contains ctrl symbols
        for i in "[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]":
            self.assertIn(i, actual)

    def test_vocabulary_on_text(self):
        additional_special_tokens = ["<INT>", "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>"]
        tokenizer = BertTokenizerFast.from_pretrained("C:/Users/Meelis/PycharmProjects/medbert/data/test_model/",
                                                  do_lower_case=False,
                                                  additional_special_tokens=additional_special_tokens)
        output = tokenizer.encode("<XXX> <FLOAT> , võrreldes eelmise visiidiga <DATE> .", )
        print(output)
        print(tokenizer.decode(output))


if __name__ == '__main__':
    unittest.main()
