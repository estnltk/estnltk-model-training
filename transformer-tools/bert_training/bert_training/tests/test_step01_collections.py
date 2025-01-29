import csv
import os
import unittest
from sys import platform

import testing.postgresql
from pathlib import Path

from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpora
from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import create_schema
from testing import postgresql

from pipelines.step01_text_processing.EstNLTKCollection import clean_and_extract

# make sure that you have initdb location in your environment variables
# for example "C:\Program Files\PostgreSQL\13\bin"
# https://github.com/tk0miya/testing.postgresql/issues/35
class TextCleaningTestsCases(unittest.TestCase):
    ROOT_DIR = str(Path(__file__).parent.parent)

    def get_all_lines_from_tsv(self, file):
        res = []
        with open(file, newline='', encoding='utf-8') as f:
            tsv_reader = csv.reader(f, delimiter="\t")
            next(tsv_reader)
            for row in tsv_reader:
                res.append(row)
        return res

    def setUp(self):
        self.postgresql = testing.postgresql.Postgresql()
        self.con_info = {"host": self.postgresql.dsn()["host"],
                         "dbname": self.postgresql.dsn()["database"],
                         "port": self.postgresql.dsn()["port"],
                         "user": self.postgresql.dsn()["user"],
                         "schema": "step01_collection_test",
                         "password": ""}

        storage = PostgresStorage(**self.con_info)
        create_schema(storage)
        corp_path = self.ROOT_DIR + "/data/Horisont/Hori/horisont"
        collection = storage['horisont_collection'].create('Horisont demo',
                                                           meta={
                                                               'type': 'str',
                                                               "title": 'str',
                                                               "author": 'str',
                                                               'ajakirjanumber': 'str'
                                                           })
        with collection.insert() as collection_insert:
            for text_obj in parse_tei_corpora(corp_path, target=['artikkel']):
                collection_insert(text_obj, meta_data=text_obj.meta)

    def tearDown(self):
        self.postgresql.stop()

    def test_corpus_to_bert_input_pipeline_clean_par(self):
        actual_file = self.ROOT_DIR + "/data/collection_clean_test.tsv"
        exp_file = self.ROOT_DIR + "/data/corp_res_no_clean_exp.tsv"
        clean_and_extract(self.con_info, 'horisont_collection', actual_file,
                          max_workers=2, clean=None, verbose=True)
        actual = self.get_all_lines_from_tsv(actual_file)
        expected = self.get_all_lines_from_tsv(exp_file)
        self.assertEqual(len(actual), len(expected))


if __name__ == '__main__':
    unittest.main()
