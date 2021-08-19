import csv
import unittest
from pathlib import Path

from pipelines.step01_create_training_corpus.estNLTK_corpus_parallel_processing import \
    clean_and_extract_parallel_corpus
from pipelines.step01_create_training_corpus.tsv_parallel_processing import \
    clean_and_extract_parallel_tsv

from pipelines.step01_create_training_corpus.estNLTK_corpus_processing import clean_and_extract_sentences_corpus
from pipelines.step01_create_training_corpus.tsv_processing import clean_and_extract_sentences_tsv

from pipelines.step01_create_training_corpus.textprocessing.text_cleaning import clean_med, clean_med_r_events


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

    # CORPUS
    def test_corpus_to_bert_input_pipeline_clean_none(self):
        corp_path = self.ROOT_DIR + "/data/Horisont/Hori/horisont"
        out_file_path = self.ROOT_DIR + "/data/corp_res_no_clean.tsv"
        exp_file_path = self.ROOT_DIR + "/data/corp_res_no_clean_exp.tsv"
        clean_and_extract_sentences_corpus(corp_path, out_file_path, clean=None)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        self.assertEqual(len(actual), len(expected))

    def test_corpus_to_bert_input_pipeline_clean(self):
        corp_path = self.ROOT_DIR + "/data/Horisont/Hori/horisont"
        out_file_path = self.ROOT_DIR + "/data/corp_res_clean.tsv"
        exp_file_path = self.ROOT_DIR + "/data/corp_res_clean_exp.tsv"
        clean_and_extract_sentences_corpus(corp_path, out_file_path, clean=clean_med)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        self.assertEqual(len(actual), len(expected))

    def test_corpus_to_bert_input_pipeline_clean_events(self):
        corp_path = self.ROOT_DIR + "/data/Horisont/Hori/horisont"
        out_file_path = self.ROOT_DIR + "/data/corp_res_clean_r_events.tsv"
        exp_file_path = self.ROOT_DIR + "/data/corp_res_clean_r_events_exp.tsv"
        clean_and_extract_sentences_corpus(corp_path, out_file_path, clean=clean_med_r_events)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        self.assertEqual(len(actual), len(expected))

    # TSV
    def tsv_to_bert_input_pipeline(self, input, output, exp_path, clean, text_col_i=1):
        clean_and_extract_sentences_tsv(input, output, clean=clean, text_col_i=text_col_i)
        actual = self.get_all_lines_from_tsv(output)
        expected = self.get_all_lines_from_tsv(exp_path)
        self.assertEqual(actual, expected)

    def test_tsv_to_bert_input_pipeline_clean_none(self):
        corp_path = self.ROOT_DIR + "/data/egcut_epi_mperli_texts_template.tsv"
        corp_path2 = self.ROOT_DIR + "/data/egcut_epi_mperli_texts_template_text_only.tsv"
        out_file_path = self.ROOT_DIR + "/data/tsv_res_no_clean.tsv"
        exp_file_path = self.ROOT_DIR + "/data/tsv_res_no_clean_exp.tsv"
        self.tsv_to_bert_input_pipeline(corp_path, out_file_path, exp_file_path, None, 1)
        self.tsv_to_bert_input_pipeline(corp_path2, out_file_path, exp_file_path, None, 0)

    def test_tsv_to_bert_input_pipeline_clean(self):
        corp_path = self.ROOT_DIR + "/data/egcut_epi_mperli_texts_template.tsv"
        corp_path2 = self.ROOT_DIR + "/data/egcut_epi_mperli_texts_template_text_only.tsv"
        out_file_path = self.ROOT_DIR + "/data/tsv_res_clean.tsv"
        exp_file_path = self.ROOT_DIR + "/data/tsv_res_clean_exp.tsv"
        self.tsv_to_bert_input_pipeline(corp_path, out_file_path, exp_file_path, clean_med, 1)
        self.tsv_to_bert_input_pipeline(corp_path2, out_file_path, exp_file_path, clean_med, 0)

    def test_tsv_to_bert_input_pipeline_clean_events(self):
        corp_path = self.ROOT_DIR + "/data/egcut_epi_mperli_texts_template.tsv"
        corp_path2 = self.ROOT_DIR + "/data/egcut_epi_mperli_texts_template_text_only.tsv"
        out_file_path = self.ROOT_DIR + "/data/tsv_res_clean_r_events.tsv"
        exp_file_path = self.ROOT_DIR + "/data/tsv_res_clean_r_events_exp.tsv"
        self.tsv_to_bert_input_pipeline(corp_path, out_file_path, exp_file_path, clean_med_r_events, 1)
        self.tsv_to_bert_input_pipeline(corp_path2, out_file_path, exp_file_path, clean_med_r_events, 0)

    # parallel
    def test_tsv_to_bert_input_pipeline_clean_par(self):
        corp_path = self.ROOT_DIR + "/data/egcut_epi_mperli_texts_template.tsv"
        out_file_path = self.ROOT_DIR + "/data/tsv_res_clean_r_events_par.tsv"
        exp_file_path = self.ROOT_DIR + "/data/tsv_res_clean_r_events_exp.tsv"
        clean_and_extract_parallel_tsv(corp_path, 1, out_file_path, max_processes=8, clean=clean_med_r_events)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        # testing against single thread processing. Since the order of texts is not the same, the lengths are compared
        self.assertEqual(len(actual), len(expected))

    def test_corpus_to_bert_input_pipeline_clean_par(self):
        corp_path = self.ROOT_DIR + "/data/Horisont/Hori/horisont"
        out_file_path = self.ROOT_DIR + "/data/corp_res_clean_r_events_par.tsv"
        exp_file_path = self.ROOT_DIR + "/data/corp_res_clean_r_events_exp.tsv"
        clean_and_extract_parallel_corpus(corp_path, out_file_path, max_processes=8, clean=clean_med_r_events)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        self.assertEqual(len(actual), len(expected))


if __name__ == '__main__':
    unittest.main()
