import os
import shutil
import unittest
from os.path import isdir, exists
from pathlib import Path

from transformers import TrainingArguments, BertForPreTraining, BertTokenizerFast, BertConfig
import torch

from pipelines.step02_BERT_pre_training.pre_train_BERT import pre_train_BERT

# These tests are slow, since training is slow. We ignore them by default ('0'),
# and run only when explicitly asked (RUN_SLOW_TESTS=1 python -m unittest ...)
RUN_SLOW_TESTS = int(os.getenv('RUN_SLOW_TESTS', '0'))


# To execute slow tests use for example:
#   RUN_SLOW_TESTS=1 python -m unittest tests/test_step02_pipeline.py
@unittest.skipIf(not RUN_SLOW_TESTS, "Warning! These tests are slow, since training is slow")
class TextCleaningTestsCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def tearDown(self):
        shutil.rmtree(self.model_path)

    def check_if_model_files_exist(self, model_path):
        self.assertTrue(exists(model_path + "/config.json"))
        self.assertTrue(exists(model_path + "/pytorch_model.bin"))
        self.assertTrue(exists(model_path + "/special_tokens_map.json"))
        self.assertTrue(exists(model_path + "/tokenizer.json"))
        self.assertTrue(exists(model_path + "/tokenizer_config.json"))
        self.assertTrue(exists(model_path + "/training_args.bin"))
        self.assertTrue(exists(model_path + "/vocab.txt"))

    def test_pretraining_minimal(self):
        """
        Tests if a model can be trained by only providing model path and the training files
        """

        torch.cuda.empty_cache()
        self.model_path = self.ROOT_DIR + "/data/test_model_step02_minimal"
        training_files = [self.ROOT_DIR + "/data/corp_res_clean_r_events_par.tsv"]

        # if dir already exists, then delete it
        if isdir(self.model_path):
            shutil.rmtree(self.model_path)

        self.assertFalse(isdir(self.model_path))

        pre_train_BERT(self.model_path, training_files)

        # checking if it created a new model
        self.assertTrue(isdir(self.model_path))

        # checking if model files were created
        self.check_if_model_files_exist(self.model_path)

        # checking if it created a checkpoint
        self.assertTrue(isdir(self.model_path + "/checkpoint-500"))

        # checking if the model can be loaded
        model = BertForPreTraining.from_pretrained(self.model_path)
        self.assertTrue(isinstance(model, BertForPreTraining))

        # checking if the tokenizer can be loaded
        tokenizer = BertTokenizerFast.from_pretrained(self.model_path)
        self.assertTrue(isinstance(tokenizer, BertTokenizerFast))


    def test_pretraining_with_more_params(self):
        self.model_path = self.ROOT_DIR + "/data/test_model_step02_params"

        # if dir already exists, then delete it
        if isdir(self.model_path):
            shutil.rmtree(self.model_path)

        self.assertFalse(isdir(self.model_path))

        # NOTE: While running tests we see the output (with transformers 4.8.1):
        #   test_pretraining_with_more_params (tests.test_step02_pipeline.TextCleaningTestsCases) ...
        #   PyTorch: setting up devices
        #   The default value for the training argument `--report_to` will change
        #   in v5 (from all installed integrations to none).
        #   In v5, you will need to use `--report_to all` to get the same behavior as now.
        #   You should start updating your code and make this info disappear :-).
        training_args = TrainingArguments(
            output_dir=self.model_path,
            overwrite_output_dir=True,
            num_train_epochs=1,
            per_device_train_batch_size=8,
            per_device_eval_batch_size=8,
        )

        torch.cuda.empty_cache()
        special_tokens = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"]
        additional_special_tokens = ["<INT>", "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>", "<br>"]
        training_files = [self.ROOT_DIR + "/data/corp_res_clean_r_events_par.tsv"]

        # just a sample config. These params arent usually used.
        config = BertConfig(
            hidden_size=480,
            max_position_embeddings=1024,
            vocab_size=1234,
        )
        pre_train_BERT(self.model_path,
                       training_files,
                       training_args=training_args, bert_config=config,
                       vocab_size=4000, lowercase=False, max_length=128, truncation=True, padding=True,
                       special_tokens=special_tokens, additional_special_tokens=additional_special_tokens)

        # checking if model files were created
        self.check_if_model_files_exist(self.model_path)

        tokenizer = BertTokenizerFast.from_pretrained(self.model_path)
        self.assertEqual(4000, len(tokenizer.get_vocab().keys()))

        # testing that the configs match and vocab size is taken from argument
        conf = BertConfig.from_pretrained(self.model_path)
        config.vocab_size = 4000
        self.assertDictEqual(conf.to_dict(), config.to_dict())

        # checking if the model can be loaded
        model = BertForPreTraining.from_pretrained(self.model_path)
        self.assertTrue(isinstance(model, BertForPreTraining))



if __name__ == '__main__':
    unittest.main()
