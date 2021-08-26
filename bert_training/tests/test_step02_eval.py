import unittest
import random
from pathlib import Path
import transformers

from pipelines.step02_BERT_pre_training.eval_BERT import eval_pretrained_BERT


class PretrainedModelEvaluation(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def test_pre_trained_model_eval(self):
        # Have to set a seed, so the outcome would always be the same
        random.seed(42)
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

        m_exp = {'eval_loss': 4.96313, 'eval_accuracy': 0.36036, 'eval_precision': 0.35735, 'eval_recall': 0.36036,
                 'eval_f1': 0.35525}
        n_exp = {'eval_loss': 0.68867, 'eval_accuracy': 0.4, 'eval_precision': 0.37222, 'eval_recall': 0.4,
                 'eval_f1': 0.34258}

        for k in m_exp.keys():
            self.assertEqual(round(m[k], 4), round(m_exp[k], 4), f"mlm At {k}")
            self.assertEqual(round(n[k], 4), round(n_exp[k], 4), f"nsp At {k}")


if __name__ == '__main__':
    unittest.main()
