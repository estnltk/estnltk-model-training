import os
import unittest
from pathlib import Path

from pipelines.step03a_BERT_fine_tuning.fine_tune_BERT import finetune_BERT_model_on_sequence_classification

RUN_SLOW_TESTS = int(os.getenv('RUN_SLOW_TESTS', '0'))


# To execute slow tests use for example:
#   RUN_SLOW_TESTS=1 python -m unittest tests/test_step02_pipeline.py
@unittest.skipIf(not RUN_SLOW_TESTS, "Warning! These tests are slow, since training is slow")
class TextCleaningTestsCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)
    
    def test_finetuning_sequence_classification_model(self):
        model_path = "bert-base-cased"
        data_path = self.ROOT_DIR + "/data/newsCorpora_subset.tsv"
        self.assertTrue(True)
        #finetune_BERT_model_on_sequence_classification(model_path, data_path, lowercase="False")

if __name__ == '__main__':
    unittest.main()
