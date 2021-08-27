import unittest
from pathlib import Path
from transformers import AutoTokenizer

from pipelines.step03a_BERT_fine_tuning import SequenceClassification


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


if __name__ == '__main__':
    unittest.main()
