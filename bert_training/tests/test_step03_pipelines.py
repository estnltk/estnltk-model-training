import os
import shutil
import unittest
from pathlib import Path

from transformers import BertForSequenceClassification
from transformers.file_utils import PaddingStrategy

from pipelines.step03_BERT_fine_tuning.SequenceClassification import finetune_BERT

RUN_SLOW_TESTS = int(os.getenv('RUN_SLOW_TESTS', '0'))


# Warning! These tests download "bert-base-cased" model (436MB)
# To execute slow tests use for example:
#   RUN_SLOW_TESTS=1 python -m unittest tests/test_step02_pipeline.py
@unittest.skipIf(not RUN_SLOW_TESTS, "Warning! These tests are slow, since training is slow")
class TextCleaningTestsCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def tearDown(self) -> None:
        shutil.rmtree(self.output_dir)

    def test_finetuning_sequence_classification_model(self):
        # setting parameters
        model_path = "bert-base-cased"
        self.output_dir = self.ROOT_DIR + "/data/test_model_step03_seq_classification"
        input_files = [self.ROOT_DIR + "/data/newsCorpora_subset.tsv"]

        dataset_args = {
            "train_data_paths": input_files,
            "eval_data_paths": input_files,
            "text_col": "text",
            "y_col": "category",
            "skiprows": 0,
            "delimiter": "\t"
        }
        map_args = {
            "batched": True,
        }
        tokenizer_args = {
            "lowercase": False,
        }
        tokenization_args = {
            "max_length": 128,
            "padding": True,
            "truncation": True
        }
        training_args = {
            "output_dir": self.output_dir,
            "overwrite_output_dir": True,
            "num_train_epochs": 1,
            "per_device_train_batch_size": 8,
            "per_device_eval_batch_size": 8
        }

        # fine-tuning the model
        res = finetune_BERT(model_path, True, dataset_args, map_args, tokenizer_args, tokenization_args, training_args)

        model = BertForSequenceClassification.from_pretrained(self.output_dir)

        # testing that the labels and their id's were saved
        class_to_index = {'b': 0, 'e': 1, 'm': 2, 't': 3}
        index_to_class = {0: "b", 1: 'e', 2: 'm', 3: 't'}
        self.assertDictEqual(model.config.label2id, class_to_index)
        self.assertDictEqual(model.config.id2label, index_to_class)

        # checking if keys exist in ds.
        for k in ['eval_loss', 'eval_accuracy', 'eval_precision', 'eval_recall', 'eval_f1']:
            self.assertTrue(k in res.keys())

    def test_eval_fine_tuned_sequence_classification_model(self):
        # setting parameters
        model_path = "bert-base-cased"
        self.output_dir = self.ROOT_DIR + "/data/test_model_step03_seq_classification"
        input_files = [self.ROOT_DIR + "/data/newsCorpora_subset.tsv"]

        dataset_args = {
            "train_data_paths": input_files,
            # "eval_data_paths": input_files,
            "text_col": "text",
            "y_col": "category",
            "skiprows": 0,
            "delimiter": "\t"
        }
        map_args = {
            "batched": True,
        }
        tokenizer_args = {
            "lowercase": False,
        }
        tokenization_args = {
            "max_length": 128,
            "padding": True,
            "truncation": True
        }
        training_args = {
            "output_dir": self.output_dir,
            "overwrite_output_dir": True,
            "num_train_epochs": 1,
            "per_device_train_batch_size": 8,
            "per_device_eval_batch_size": 8
        }

        # First fine-tuning the model
        finetune_BERT(model_path, True, dataset_args, map_args, tokenizer_args, tokenization_args, training_args)

        # creating a new dataset_args dict to use eval dataset
        dataset_args = {
            "eval_data_paths": input_files,
            "text_col": "text",
            "y_col": "category",
            "skiprows": 0,
            "delimiter": "\t"
        }
        res = finetune_BERT(self.output_dir, True, dataset_args, map_args, tokenizer_args, tokenization_args,
                            training_args)

        # checking if keys exist in ds.
        for k in ['eval_loss', 'eval_accuracy', 'eval_precision', 'eval_recall', 'eval_f1']:
            self.assertTrue(k in res.keys())


if __name__ == '__main__':
    unittest.main()
