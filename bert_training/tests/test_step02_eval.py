
import unittest
import random
from pipelines.step02_BERT_pre_training.eval_BERT import eval_pretrained_BERT


class PretrainedModelEvaluation(unittest.TestCase):

    def test_pre_trained_model_eval(self):
        random.seed(42)
        model_path = "tartuNLP/EstBERT"
        input_files = ["../data/model_eval_texts.tsv"]

        tokenizer_args = {
            "lowercase": False,
        }
        tokenization_args = {
            "max_length": 128,
            "padding": True,
            "truncation": True
        }

        m, n = eval_pretrained_BERT(model_path, input_files, tokenizer_args=tokenizer_args, tokenization_args=tokenization_args)
        m_exp = {'eval_loss': 4.963132381439209, 'eval_accuracy': 0.36036036036036034, 'eval_precision': 0.3573573573573574, 'eval_recall': 0.36036036036036034, 'eval_f1': 0.3552552552552553, 'eval_runtime': 0.6754, 'eval_samples_per_second': 22.208, 'eval_steps_per_second': 2.961}
        n_exp = {'eval_loss': 0.6886705756187439, 'eval_accuracy': 0.4, 'eval_precision': 0.3722222222222223, 'eval_recall': 0.4, 'eval_f1': 0.34258373205741627, 'eval_runtime': 0.077, 'eval_samples_per_second': 194.804, 'eval_steps_per_second': 25.974}

        keys = ['eval_loss', 'eval_accuracy', 'eval_precision', 'eval_recall', 'eval_f1']
        for k in keys:
            self.assertAlmostEquals(m[k], m_exp[k], 2, f"mlm At {k}")
            self.assertAlmostEquals(n[k], n_exp[k], 2, f"nsp At {k}")

if __name__ == '__main__':
    unittest.main()
