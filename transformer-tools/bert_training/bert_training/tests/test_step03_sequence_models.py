import csv
import os
import shutil
import unittest
from sys import platform

import testing.postgresql
from pathlib import Path

from datasets import load_dataset
from estnltk import Text
from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpora
from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import create_schema
from transformers import AutoTokenizer

from pipelines.step03_BERT_fine_tuning import SequenceClassification
from pipelines.step03_BERT_fine_tuning.SequenceClassification import finetune_BERT, evaluate
from pipelines.step03_BERT_fine_tuning.dataloaders import Sequences

from pipelines.step01_text_processing.EstNLTKCollection import clean_and_extract

RUN_SLOW_TESTS = int(os.getenv('RUN_SLOW_TESTS', '0'))


# make sure that you have initdb location in your environment variables
# for example "C:\Program Files\PostgreSQL\13\bin"
# https://github.com/tk0miya/testing.postgresql/issues/35

# To execute slow tests use for example:
#   RUN_DB_TESTS=1 python -m unittest tests/test_step01_collections.py
@unittest.skipIf(not RUN_SLOW_TESTS, "Warning! These tests are slow, since training is slow")
class SequenceModelTests(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def setUp(self):
        self.output_dir = ""
        self.postgresql = testing.postgresql.Postgresql()
        self.con_info = {"host": self.postgresql.dsn()["host"],
                         "dbname": self.postgresql.dsn()["database"],
                         "port": self.postgresql.dsn()["port"],
                         "user": self.postgresql.dsn()["user"],
                         "schema": "step01_collection_test",
                         "password": ""}

        self.storage = PostgresStorage(**self.con_info)
        create_schema(self.storage)

    def tearDown(self):
        self.postgresql.stop()
        if self.output_dir != "":
            shutil.rmtree(self.output_dir)

    def fill_db(self):
        collection = self.storage['step_03_seq_col_test'].create('Sequence collecton demo',
                                                                 meta={
                                                                     'label': 'str',
                                                                 })

        ds = load_dataset("csv", data_files={'train': self.ROOT_DIR + '/data/corp_res_clean_r_events_par.tsv'},
                          delimiter="\t")
        out_path = self.ROOT_DIR + "/data/step03_seq_class_horisont_example.tsv"
        with open(out_path, 'w', newline='', encoding='utf-8') as out_file:
            writer = csv.writer(out_file, delimiter="\t")
            # saving the column names
            writer.writerow(["text", "y"])
            with collection.insert() as collection_insert:
                for document in ds['train']['text']:
                    for sentence in document.split("\n"):
                        # saving sentences and labels to a file
                        label = 1 if "<DATE>" in sentence else 0
                        meta = {'label': label}
                        collection_insert(Text(sentence), meta_data=meta)

    def test_estNLTK_collection_to_bert_ft_input(self):
        # Init
        self.fill_db()

        # Testing
        dl = Sequences.EstNLTKCol(self.con_info, "step_03_seq_col_test", X="text", y="label")
        tokenizer = AutoTokenizer.from_pretrained("tartuNLP/EstBERT", lowercase="False")

        tokenization_args = {
            "max_length": 128,
            "truncation": True,
            "padding": "max_length"
        }

        map_args = {
            "batched": True,
        }
        ds, a, b = SequenceClassification.encode_dataset(tokenizer, dl,
                                                         tokenization_args=tokenization_args, map_args=map_args)
        a2 = {'0': 0, '1': 1}
        b2 = {0: '0', 1: '1'}
        self.assertDictEqual(a, a2)
        self.assertDictEqual(b, b2)
        self.assertEqual(2021, len(ds))

    def test_fine_tune_eval_sequence_classification_estNLTK_collection(self):
        # Init
        self.output_dir = self.ROOT_DIR + "/data/step03_sequence_class_test_model"
        self.fill_db()

        # Testing
        dl = Sequences.EstNLTKCol(self.con_info, "step_03_seq_col_test", X="text", y="label")

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

        finetune_BERT("tartuNLP/EstBERT", dl, True, map_args, tokenizer_args,
                      tokenization_args, training_args)

        res = evaluate(self.output_dir, dl, map_args, tokenizer_args,
                       tokenization_args, training_args)

        for k in ['eval_loss', 'eval_accuracy', 'eval_precision', 'eval_recall', 'eval_f1']:
            self.assertTrue(k in res.keys())

    # -------------------
    # TSV
    # -------------------
    def test_TSV_to_bert_ft_input(self):
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

        dl = Sequences.Tsv(
            input_file,
            X="text",
            y="category"
        )

        ds, a, b = SequenceClassification.encode_dataset(tokenizer, dl,
                                                         tokenization_args=tokenization_args, map_args=map_args)
        a2 = {'b': 0, 'e': 1, 'm': 2, 't': 3}
        b2 = {0: "b", 1: 'e', 2: 'm', 3: 't'}
        self.assertDictEqual(a, a2)
        self.assertDictEqual(b, b2)
        self.assertEqual(1000, len(ds))

    def test_fine_tune_eval_sequence_classification_TSV(self):
        # setting parameters
        model_path = "bert-base-cased"
        self.output_dir = self.ROOT_DIR + "/data/test_model_step03_seq_classification"
        input_files = [self.ROOT_DIR + "/data/newsCorpora_subset.tsv"]

        dl = Sequences.Tsv(
            input_files,
            X="text",
            y="category"
        )
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
        model = SequenceClassification.finetune_BERT(model_path, dl, True, map_args, tokenizer_args,
                                             tokenization_args, training_args)

        # testing that the labels and their id's were saved
        class_to_index = {'b': 0, 'e': 1, 'm': 2, 't': 3}
        index_to_class = {0: "b", 1: 'e', 2: 'm', 3: 't'}
        self.assertDictEqual(model.config.label2id, class_to_index)
        self.assertDictEqual(model.config.id2label, index_to_class)

        res = SequenceClassification.evaluate(self.output_dir, dl, map_args, tokenizer_args,
                                              tokenization_args,
                                              training_args)

        # checking if keys exist in ds.
        for k in ['eval_loss', 'eval_accuracy', 'eval_precision', 'eval_recall', 'eval_f1']:
            self.assertTrue(k in res.keys())


if __name__ == '__main__':
    unittest.main()
