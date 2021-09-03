import unittest
from pathlib import Path
from transformers import AutoTokenizer

from pipelines.step03_BERT_fine_tuning import SequenceClassification
from pipelines.step03_BERT_fine_tuning import TokenClassification


class step03DatasetTestsCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def test_sequence_classification_ds_no_files(self):
        tokenizer = AutoTokenizer.from_pretrained("bert-base-cased", lowercase="False")

        ds, a, b = SequenceClassification.encode_dataset(tokenizer, text_col="text", y_col="category",
                                                         tokenization_args=None, map_args=None)

        self.assertEqual(None, ds)
        self.assertEqual(None, a)
        self.assertEqual(None, b)

    def test_sequence_classification_ds(self):
        tokenizer = AutoTokenizer.from_pretrained("bert-base-cased", lowercase="False")
        input_file = self.ROOT_DIR + "/data/newsCorpora_subset.tsv"

        tokenization_args = {
            "max_length": 128,
            "truncation": True,
            "padding": True
        }

        map_args = {
            "batched": True,
        }

        ds, a, b = SequenceClassification.encode_dataset(tokenizer, input_file, eval_data_paths=input_file,
                                                         text_col="text", y_col="category",
                                                         tokenization_args=tokenization_args, map_args=map_args)
        a2 = {'b': 0, 'e': 1, 'm': 2, 't': 3}
        b2 = {0: "b", 1: 'e', 2: 'm', 3: 't'}
        self.assertDictEqual(a, a2)
        self.assertDictEqual(b, b2)
        self.assertEqual(1000, len(ds['train']))
        self.assertEqual(1000, len(ds['eval']))

    def test_sequence_classification_ds_custom_indexes(self):
        tokenizer = AutoTokenizer.from_pretrained("bert-base-cased", lowercase="False")
        input_file = self.ROOT_DIR + "/data/newsCorpora_subset.tsv"

        tokenization_args = {
            "max_length": 128,
            "truncation": True,
            "padding": True
        }

        map_args = {
            "batched": True,
        }
        class_to_index = {'b': 1, 'e': 3, 'm': 0, 't': 2}
        index_to_class = {1: "b", 3: 'e', 0: 'm', 2: 't'}

        ds, a, b = SequenceClassification.encode_dataset(tokenizer, input_file, eval_data_paths=input_file,
                                                         text_col="text", y_col="category",
                                                         class_to_index=class_to_index,
                                                         tokenization_args=tokenization_args, map_args=map_args)

        self.assertDictEqual(a, class_to_index)
        self.assertDictEqual(b, index_to_class)

    def test_token_classification_ds(self):
        tokenizer = AutoTokenizer.from_pretrained("tartuNLP/EstBERT", lowercase="False")
        input_file = self.ROOT_DIR + "/data/step03_tok_class_horisont_example.tsv"

        tokenization_args = {
            "max_length": 128,
            "truncation": True,
            "padding": "max_length",
        }

        map_args = {
            "batched": True,
        }
        class_to_index = {'B_A': 0, 'B_C': 1, 'B_D': 2, 'B_G': 3, 'B_H': 4, 'B_I': 5, 'B_J': 6, 'B_K': 7, 'B_N': 8,
                          'B_O': 9, 'B_P': 10, 'B_S': 11, 'B_U': 12, 'B_V': 13, 'B_X': 14, 'B_Y': 15, 'B_Z': 16,
                          'I_A': 17, 'I_D': 18, 'I_H': 19, 'I_N': 20, 'I_P': 21, 'I_S': 22, 'I_V': 23, 'I_Z': 24,
                          'O_A': 25, 'O_D': 26, 'O_H': 27, 'O_I': 28, 'O_J': 29, 'O_K': 30, 'O_N': 31, 'O_P': 32,
                          'O_S': 33, 'O_V': 34, 'O_Y': 35, 'O_Z': 36}

        index_to_class = {0: 'B_A', 1: 'B_C', 2: 'B_D', 3: 'B_G', 4: 'B_H', 5: 'B_I', 6: 'B_J', 7: 'B_K', 8: 'B_N',
                          9: 'B_O', 10: 'B_P', 11: 'B_S', 12: 'B_U', 13: 'B_V', 14: 'B_X', 15: 'B_Y', 16: 'B_Z',
                          17: 'I_A', 18: 'I_D', 19: 'I_H', 20: 'I_N', 21: 'I_P', 22: 'I_S', 23: 'I_V', 24: 'I_Z',
                          25: 'O_A', 26: 'O_D', 27: 'O_H', 28: 'O_I', 29: 'O_J', 30: 'O_K', 31: 'O_N', 32: 'O_P',
                          33: 'O_S', 34: 'O_V', 35: 'O_Y', 36: 'O_Z'}

        ds, a, b = TokenClassification.encode_dataset(tokenizer, input_file,
                                                      tokenization_args=tokenization_args,
                                                      map_args=map_args)

        for labs, toks in zip(ds['train']['labels'], ds['train']['input_ids']):
            for lab, tok in zip(labs, toks):
                # Asserting that sub-word units have label -100
                tok = tokenizer.decode(tok)
                if tok[:2] == "##" or tok in {"[CLS]", "[SEP]", "[PAD]"}:
                    self.assertTrue(lab == -100)


        self.assertDictEqual(a, class_to_index)
        self.assertDictEqual(b, index_to_class)


    def test_token_classification_ds_with_label_all(self):
        tokenizer = AutoTokenizer.from_pretrained("tartuNLP/EstBERT", lowercase="False")
        input_file = self.ROOT_DIR + "/data/step03_tok_class_horisont_example.tsv"

        tokenization_args = {
            "max_length": 128,
            "truncation": True,
            "padding": True
        }

        map_args = {
            "batched": True,
        }

        ds, a, b = TokenClassification.encode_dataset(tokenizer, input_file, label_all_subword_units=True,
                                                      eval_data_paths=input_file,
                                                      tokenization_args=tokenization_args,
                                                      map_args=map_args, skiprows=1)
        for labs, toks in zip(ds['train']['labels'], ds['train']['input_ids']):
            for lab, tok in zip(labs, toks):
                # Asserting that sub-word units have label -100
                tok = tokenizer.decode(tok)
                if tok[:2] == "##":
                    self.assertTrue(lab != -100)

    def test_token_classification_ds_reading_with_and_without_headers(self):
        tokenizer = AutoTokenizer.from_pretrained("tartuNLP/EstBERT", lowercase="False")
        input_file = self.ROOT_DIR + "/data/step03_tok_class_horisont_example.tsv"
        input_file2 = self.ROOT_DIR + "/data/step03_tok_class_horisont_example_no_header.tsv"

        tokenization_args = {
            "max_length": 128,
            "truncation": True,
            "padding": True
        }

        map_args = {
            "batched": True,
        }

        ds, a, b = TokenClassification.encode_dataset(tokenizer, input_file, label_all_subword_units=True,
                                                      tokenization_args=tokenization_args,
                                                      map_args=map_args)

        ds2, a, b = TokenClassification.encode_dataset(tokenizer, input_file2, has_header_col=False, text_col=0,
                                                       y_col=1, label_all_subword_units=True,
                                                       tokenization_args=tokenization_args,
                                                       map_args=map_args)
        self.assertEqual(len(ds['train']), len(ds2['train']))


if __name__ == '__main__':
    unittest.main()
