import unittest

from pipelines.step02_BERT_pre_training.tokenization.vocabulary_creator import create_vocabulary


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

    def vocabulary_creation_testing(self, mode):
        vocab_path = "C:/Users/Meelis/PycharmProjects/medbert/data/model_{}.txt".format(mode)
        input = "C:/Users/Meelis/PycharmProjects/medbert/data/corp_res_clean_r_events_exp.txt"
        size = 3000
        create_vocabulary(vocab_path, size, input, mode=mode)
        actual = load_test_file(vocab_path)

        # testing that vocabulary size and the argument size are equal
        self.assertEqual(size, file_len(vocab_path))

        # testing that the vocabulary contains ctrl symbols
        for i in "[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]":
            self.assertIn(i, actual)


    def test_vocabulary_creation_unigram(self):
        self.vocabulary_creation_testing("unigram")

    def test_vocabulary_creation_bpe(self):
        self.vocabulary_creation_testing("bpe")

    def test_vocabulary_creation_char(self):
        self.vocabulary_creation_testing("char")

    def test_vocabulary_creation_word(self):
        self.vocabulary_creation_testing("word")


if __name__ == '__main__':
    unittest.main()
