import unittest
import random
from pathlib import Path
import transformers

from pipelines.step02_BERT_pre_training.eval_BERT import eval_pretrained_BERT


class PretrainedModelEvaluation(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def test_pre_trained_model_eval(self):
        # disables warning, that some weights were not initialized...
        transformers.logging.set_verbosity_error()

        model_path = "tartuNLP/EstBERT"
        input_files = [self.ROOT_DIR + "/data/model_eval_texts.tsv"]

        tokenizer_args = {
            "lowercase": False,
        }
        tokenization_args = {
            "max_length": 128,
            "padding": True,
            "truncation": True
        }

        m, n = eval_pretrained_BERT(model_path, input_files, tokenizer_args=tokenizer_args,
                                    tokenization_args=tokenization_args, verbose=False)

        keys = ['eval_loss', 'eval_accuracy', 'eval_precision', 'eval_recall', 'eval_f1']

        # checking if keys exist in ds. If they do it at least checks, that all functions were called
        # had to remove checking values, that might be random

        for k in keys:
            self.assertTrue(k in m.keys())
            self.assertTrue(k in n.keys())


if __name__ == '__main__':
    unittest.main()
