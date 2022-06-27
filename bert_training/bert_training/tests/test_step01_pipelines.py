import csv
import unittest

from pathlib import Path

from pipelines.step01_text_processing import corpus, tsv
from pipelines.step01_text_processing.textprocessing.text_cleaning import clean_med, clean_med_events


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
        corpus.clean_and_extract(corp_path, out_file_path, clean=None)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        self.assertEqual(len(actual), len(expected))

    def test_corpus_to_bert_input_pipeline_clean(self):
        corp_path = self.ROOT_DIR + "/data/Horisont/Hori/horisont"
        out_file_path = self.ROOT_DIR + "/data/corp_res_clean.tsv"
        exp_file_path = self.ROOT_DIR + "/data/corp_res_clean_exp.tsv"
        corpus.clean_and_extract(corp_path, out_file_path, clean=clean_med)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        self.assertEqual(len(actual), len(expected))

    def test_corpus_to_bert_input_pipeline_clean_events(self):
        corp_path = self.ROOT_DIR + "/data/Horisont/Hori/horisont"
        out_file_path = self.ROOT_DIR + "/data/corp_res_clean_r_events.tsv"
        exp_file_path = self.ROOT_DIR + "/data/corp_res_clean_r_events_exp.tsv"
        corpus.clean_and_extract(corp_path, out_file_path, clean=clean_med_events)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        self.assertEqual(len(actual), len(expected))

    # TSV
    def tsv_to_bert_input_pipeline(self, input, output, exp_path, clean, text_col_i=1):
        tsv.clean_and_extract(input, output, clean=clean, text_col_i=text_col_i)
        actual = self.get_all_lines_from_tsv(output)
        expected = self.get_all_lines_from_tsv(exp_path)
        self.assertEqual(actual, expected)

    # In following tests the text part is either from multi-column files or from single-column file,
    # in both cases column is titled as 'text'. Text itself looks like so:
    #   "24.02.2015 - Kaebusteta
    #   19.02.2015 -
    #    Tööstaaž: praegu töötab tarkvara arendajana . Tööstaaž selles töökohas - 4  a., üldine -5,5  a. varasemad ametid - samalaadne"
    # And output, depending from cleaning method should be either:
    # clean_med:
    #   "<DATE> - Kaebusteta <br> <DATE> - <br> Tööstaaž : praegu töötab tarkvara arendajana .
    #   Tööstaaž selles töökohas - <INT> a. , üldine - <FLOAT> a. varasemad ametid - samalaadne"
    # clean_med_events:
    #   "Kaebusteta
    #   Tööstaaž : praegu töötab tarkvara arendajana .
    #   Tööstaaž selles töökohas - <INT> a. , üldine - <FLOAT> a. varasemad ametid - samalaadne"
    #
    # Actual tested inputs are a bit larger.
    # Main tested method is pipelines/step01_text_processing/tsv.py -> clean_and_extract(...)
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
        self.tsv_to_bert_input_pipeline(corp_path, out_file_path, exp_file_path, clean_med_events, 1)
        self.tsv_to_bert_input_pipeline(corp_path2, out_file_path, exp_file_path, clean_med_events, 0)

    # parallel
    def test_tsv_to_bert_input_pipeline_clean_par(self):
        corp_path = self.ROOT_DIR + "/data/egcut_epi_mperli_texts_template.tsv"
        out_file_path = self.ROOT_DIR + "/data/tsv_res_clean_r_events_par.tsv"
        exp_file_path = self.ROOT_DIR + "/data/tsv_res_clean_r_events_exp.tsv"
        tsv.clean_and_extract(corp_path, out_file_path, 1, max_processes=8, clean=clean_med_events)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        # testing against single thread processing. Since the order of texts is not the same, the lengths are compared
        self.assertEqual(len(actual), len(expected))

    def test_corpus_to_bert_input_pipeline_clean_par(self):
        corp_path = self.ROOT_DIR + "/data/Horisont/Hori/horisont"
        out_file_path = self.ROOT_DIR + "/data/corp_res_clean_r_events_par.tsv"
        exp_file_path = self.ROOT_DIR + "/data/corp_res_clean_r_events_exp.tsv"
        corpus.clean_and_extract(corp_path, out_file_path, max_processes=8, clean=clean_med_events)
        actual = self.get_all_lines_from_tsv(out_file_path)
        expected = self.get_all_lines_from_tsv(exp_file_path)
        self.assertEqual(len(actual), len(expected))


if __name__ == '__main__':
    unittest.main()
