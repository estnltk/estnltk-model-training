import os
import shutil
import unittest
from os.path import isdir, exists
from pathlib import Path

from transformers import TrainingArguments, IntervalStrategy, BertForPreTraining, BertTokenizerFast, BertConfig
import torch

from pipelines.step02_BERT_pre_training.pre_train_BERT import pre_train_BERT


# Warning! These tests are slow, since training is slow
class TextCleaningTestsCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

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
        model_path = self.ROOT_DIR + "/data/test_model_1234125352"
        training_files = [self.ROOT_DIR + "/data/corp_res_clean_r_events_par.tsv"]

        if isdir(model_path):
            shutil.rmtree(model_path)

        self.assertFalse(isdir(model_path))

        pre_train_BERT(model_path, training_files)

        # checking if it created a new model
        self.assertTrue(isdir(model_path))

        # checking if model files were created
        self.check_if_model_files_exist(model_path)

        # checking if it created a checkpoint
        self.assertTrue(isdir(model_path + "/checkpoint-500"))

        # checking if the model can be loaded
        model = BertForPreTraining.from_pretrained(model_path)
        self.assertTrue(isinstance(model, BertForPreTraining))

        # checking if the tokenizer can be loaded
        tokenizer = BertTokenizerFast.from_pretrained(model_path)
        self.assertTrue(isinstance(tokenizer, BertTokenizerFast))
        shutil.rmtree(model_path)

    def test_pretraining_with_more_params(self):
        model_path = self.ROOT_DIR + "/data/test_model_1234125352"

        # if dir already exists, then delete it
        if isdir(model_path):
            shutil.rmtree(model_path)
        self.assertFalse(isdir(model_path))

        training_args = TrainingArguments(
            output_dir=model_path,
            overwrite_output_dir=True,
            num_train_epochs=1,
            per_device_train_batch_size=8,
            per_device_eval_batch_size=8,
        )

        torch.cuda.empty_cache()
        special_tokens = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"]
        additional_special_tokens = ["<INT>", "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>"]
        training_files = [self.ROOT_DIR + "/data/corp_res_clean_r_events_par.tsv"]
        config = BertConfig(
            hidden_size=480,
            max_position_embeddings=1024,
            vocab_size=1234,
        )
        pre_train_BERT(model_path,
                       training_files,
                       training_args=training_args, bert_config=config,
                       vocab_size=4000, lowercase=False, max_length=128, truncation=True, padding=True,
                       special_tokens=special_tokens, additional_special_tokens=additional_special_tokens)

        # checking if model files were created
        self.check_if_model_files_exist(model_path)

        tokenizer = BertTokenizerFast.from_pretrained(model_path)
        self.assertEqual(4000, len(tokenizer.get_vocab().keys()))

        # testing that the configs match and vocab size is taken from argument
        conf = BertConfig.from_pretrained(model_path)
        config.vocab_size = 4000
        self.assertDictEqual(conf.to_dict(), config.to_dict())

        # checking if the model can be loaded
        model = BertForPreTraining.from_pretrained(model_path)
        self.assertTrue(isinstance(model, BertForPreTraining))
        shutil.rmtree(model_path)

if __name__ == '__main__':
    unittest.main()
