
import unittest
from pathlib import Path
from transformers import AutoTokenizer

from pipelines.step03a_BERT_fine_tuning.datasets.sequence_classification import encode_sequence_classification_dataset


class step03DatasetTestsCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)
    
    def test_sentence_classification_ds(self):
        tokenizer = AutoTokenizer.from_pretrained("bert-base-cased", lowercase="False")
        input_file = self.ROOT_DIR + "/data/newsCorpora_subset.tsv"
        ds, a, b = encode_sequence_classification_dataset(input_file, tokenizer, text_col="text", y_col="category",
                                                          batched=True, max_length=128, truncation=True, padding=True)
        a2 = {'b': 0, 'e': 1, 'm': 2, 't': 3}
        b2 = {0: "b", 1: 'e', 2: 'm', 3: 't'}
        self.assertDictEqual(a, a2)
        self.assertDictEqual(b, b2)
        self.assertEqual(1000, len(ds))


if __name__ == '__main__':
    unittest.main()
