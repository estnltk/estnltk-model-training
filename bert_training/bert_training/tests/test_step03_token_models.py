import csv
import os
import shutil
import unittest
from pathlib import Path
import testing.postgresql

from estnltk import Text
from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import create_schema, delete_schema
from transformers import AutoTokenizer

from pipelines.step03_BERT_fine_tuning import TokenClassification
from pipelines.step03_BERT_fine_tuning.dataloaders import Tokens

RUN_SLOW_TESTS = int(os.getenv('RUN_SLOW_TESTS', '0'))


# Warning! These tests download "bert-base-cased" model (436MB)
# To execute slow tests use for example:
#   RUN_SLOW_TESTS=1 python -m unittest tests/test_step02_pipeline.py
@unittest.skipIf(not RUN_SLOW_TESTS, "Warning! These tests are slow, since training is slow")
class TokenModelTests(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def setUp(self) -> None:
        self.output_dir = ""
        self.postgresql = testing.postgresql.Postgresql()
        self.con_inf = {
            "host": self.postgresql.dsn()["host"],
            "dbname": self.postgresql.dsn()["database"],
            "port": self.postgresql.dsn()["port"],
            "user": self.postgresql.dsn()["user"],
            "password": "",
            "schema": 'step03_tok_collection_test'
        }
        self.storage = PostgresStorage(**self.con_inf)
        create_schema(self.storage)

    def tearDown(self) -> None:
        self.postgresql.stop()
        if self.output_dir != "":
            shutil.rmtree(self.output_dir)

    def fill_db(self):
        # creating a table
        self.collection = self.storage['token_collection'].create('Token classification demo')
        in_path = self.ROOT_DIR + '/data/corp_res_no_clean.tsv'
        with open(in_path, newline='', encoding='utf-8') as in_file:
            reader = csv.reader(in_file, delimiter="\t")
            next(reader)
            with self.collection.insert() as collection_insert:
                for row in reader:
                    for sentence in row[0].split("\n"):
                        t = Text(sentence)
                        # tagging the object
                        t = t.tag_layer(['morph_analysis'])
                        collection_insert(t, meta_data=t.meta)

    def test_token_classification_ds_EstNLTK_col(self):
        # init db
        self.fill_db()
        model_path = "tartuNLP/EstBERT"
        self.output_dir = self.ROOT_DIR + "/data/test_model_step03_token_classification"
        table_name = "token_collection"
        dl = Tokens.EstNLTKCol(self.con_inf, table_name, "morph_analysis", "text", "partofspeech")

        map_args = {
            "batched": True,
        }
        tokenizer_args = {
            "lowercase": False,
        }
        tokenization_args = {
            "max_length": 128,
            "padding": "max_length",
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
        model = TokenClassification.finetune_BERT(model_path, dl, False, True, map_args, tokenizer_args,
                                                  tokenization_args,
                                                  training_args)

        i2l = {
            "0": "A", "1": "C", "2": "D", "3": "G", "4": "H", "5": "I", "6": "J", "7": "K", "8": "N", "9": "O",
            "10": "P", "11": "S", "12": "U", "13": "V", "14": "X", "15": "Y", "16": "Z"
        }
        l2i = {v: int(k) for k, v in i2l.items()}
        i2l = {int(k): v for k, v in i2l.items()}

        self.assertDictEqual(i2l, model.config.id2label)
        self.assertDictEqual(l2i, model.config.label2id)

        res = TokenClassification.evaluate(self.output_dir, dl, False, map_args, tokenizer_args,
                                           tokenization_args,
                                           training_args)

        for k in ['eval_loss', 'eval_accuracy', 'eval_precision', 'eval_recall', 'eval_f1']:
            self.assertTrue(k in res.keys())

    # -----------------------------
    # TSV
    # -----------------------------
    def test_token_classification_ds_TSV(self):
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

        dl = Tokens.Tsv(input_file)

        ds, a, b = TokenClassification.encode_dataset(tokenizer, dl, tokenization_args=tokenization_args,
                                                      map_args=map_args)

        for labs, toks in zip(ds['labels'], ds['input_ids']):
            for lab, tok in zip(labs, toks):
                # Asserting that sub-word units have label -100
                tok = tokenizer.decode(tok)
                if tok[:2] == "##" or tok in {"[CLS]", "[SEP]", "[PAD]"}:
                    self.assertTrue(lab == -100)

        self.assertDictEqual(a, class_to_index)
        self.assertDictEqual(b, index_to_class)

    def test_token_classification_ds_with_label_all_TSV(self):
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

        dl = Tokens.Tsv(input_file, skiprows=1)
        ds, a, b = TokenClassification.encode_dataset(tokenizer, dl, label_all_subword_units=True,
                                                      tokenization_args=tokenization_args,
                                                      map_args=map_args)
        for labs, toks in zip(ds['labels'], ds['input_ids']):
            for lab, tok in zip(labs, toks):
                # Asserting that sub-word units have label -100
                tok = tokenizer.decode(tok)
                if tok[:2] == "##":
                    self.assertTrue(lab != -100)

    def test_token_classification_ds_reading_with_and_without_headers_TSV(self):
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

        dl = Tokens.Tsv(input_file)
        ds, a, b = TokenClassification.encode_dataset(tokenizer, dl, label_all_subword_units=True,
                                                      tokenization_args=tokenization_args,
                                                      map_args=map_args)
        dl = Tokens.Tsv(input_file2, has_header_col=False, X=0, y=1)
        ds2, a, b = TokenClassification.encode_dataset(tokenizer, dl, label_all_subword_units=True,
                                                       tokenization_args=tokenization_args,
                                                       map_args=map_args)
        self.assertEqual(len(ds), len(ds2))

    def test_finetuning_and_eval_of_token_classification_model_TSV(self):
        # setting parameters
        model_path = "tartuNLP/EstBERT"
        self.output_dir = self.ROOT_DIR + "/data/test_model_step03_token_classification"
        input_files = [self.ROOT_DIR + "/data/step03_tok_class_horisont_example.tsv"]
        dl = Tokens.Tsv(input_files, X="text", y="y")

        map_args = {
            "batched": True,
        }
        tokenizer_args = {
            "lowercase": False,
        }
        tokenization_args = {
            "max_length": 128,
            "padding": "max_length",
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
        TokenClassification.finetune_BERT(model_path, dl, False, True, map_args, tokenizer_args, tokenization_args,
                                          training_args)

        res = TokenClassification.evaluate(self.output_dir, dl, False, map_args, tokenizer_args,
                                           tokenization_args,
                                           training_args)

        # checking if keys exist in ds.
        for k in ['eval_loss', 'eval_accuracy', 'eval_precision', 'eval_recall', 'eval_f1']:
            self.assertTrue(k in res.keys())


if __name__ == '__main__':
    unittest.main()
