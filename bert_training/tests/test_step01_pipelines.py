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

    def get_all_lines_from_txt(self, file):
        res = []
        with open(file, encoding='utf-8') as f:
            for row in f:
                res.append(row)
        return res

    # CORPUS
    def test_corpus_to_bert_input_pipeline_clean_none(self):
        corp_path = self.ROOT_DIR + "\\data\\Horisont\\Hori\\horisont"
        out_file_path = self.ROOT_DIR + "\\data\\corp_res_no_clean.txt"
        exp_file_path = self.ROOT_DIR + "\\data\\corp_res_no_clean_exp.txt"
        clean_and_extract_sentences_corpus(corp_path, out_file_path, clean=None)
        actual = self.get_all_lines_from_txt(out_file_path)
        expected = self.get_all_lines_from_txt(exp_file_path)
        self.assertEqual(actual, expected)

    def test_corpus_to_bert_input_pipeline_clean(self):
        corp_path = self.ROOT_DIR + "\\data\\Horisont\\Hori\\horisont"
        out_file_path = self.ROOT_DIR + "\\data\\corp_res_clean.txt"
        exp_file_path = self.ROOT_DIR + "\\data\\corp_res_clean_exp.txt"
        clean_and_extract_sentences_corpus(corp_path, out_file_path, clean=clean_med)
        actual = self.get_all_lines_from_txt(out_file_path)
        expected = self.get_all_lines_from_txt(exp_file_path)
        self.assertEqual(actual, expected)

    def test_corpus_to_bert_input_pipeline_clean_events(self):
        corp_path = self.ROOT_DIR + "\\data\\Horisont\\Hori\\horisont"
        out_file_path = self.ROOT_DIR + "\\data\\corp_res_clean_r_events.txt"
        exp_file_path = self.ROOT_DIR + "\\data\\corp_res_clean_r_events_exp.txt"
        clean_and_extract_sentences_corpus(corp_path, out_file_path, clean=clean_med_r_events)
        actual = self.get_all_lines_from_txt(out_file_path)
        expected = self.get_all_lines_from_txt(exp_file_path)
        self.assertEqual(actual, expected)

    # TSV
    def tsv_to_bert_input_pipeline(self, input, output, exp_path, clean):
        clean_and_extract_sentences_tsv(input, output, clean=clean)
        actual = self.get_all_lines_from_txt(output)
        expected = self.get_all_lines_from_txt(exp_path)
        self.assertEqual(actual, expected)

    def test_tsv_to_bert_input_pipeline_clean_none(self):
        corp_path = self.ROOT_DIR + "\\data\\egcut_epi_mperli_texts_1000.tsv"
        out_file_path = self.ROOT_DIR + "\\data\\tsv_res_no_clean.txt"
        exp_file_path = self.ROOT_DIR + "\\data\\tsv_res_no_clean_exp.txt"
        self.tsv_to_bert_input_pipeline(corp_path, out_file_path, exp_file_path, None)

    def test_tsv_to_bert_input_pipeline_clean(self):
        corp_path = self.ROOT_DIR + "\\data\\egcut_epi_mperli_texts_1000.tsv"
        out_file_path = self.ROOT_DIR + "\\data\\tsv_res_clean.txt"
        exp_file_path = self.ROOT_DIR + "\\data\\tsv_res_clean_exp.txt"
        self.tsv_to_bert_input_pipeline(corp_path, out_file_path, exp_file_path, clean_med)

    def test_tsv_to_bert_input_pipeline_clean_events(self):
        corp_path = self.ROOT_DIR + "\\data\\egcut_epi_mperli_texts_1000.tsv"
        out_file_path = self.ROOT_DIR + "\\data\\tsv_res_clean_r_events.txt"
        exp_file_path = self.ROOT_DIR + "\\data\\tsv_res_clean_r_events_exp.txt"
        self.tsv_to_bert_input_pipeline(corp_path, out_file_path, exp_file_path, clean_med_r_events)

    # parallel
    def test_tsv_to_bert_input_pipeline_clean_par(self):
        corp_path = self.ROOT_DIR + "\\data\\egcut_epi_mperli_texts_1000.tsv"
        out_file_path = self.ROOT_DIR + "\\data\\tsv_res_clean_r_events_par.txt"
        exp_file_path = self.ROOT_DIR + "\\data\\tsv_res_clean_r_events_exp.txt"
        clean_and_extract_parallel_tsv(corp_path, 1, out_file_path, max_processes=8, clean=clean_med_r_events)
        actual = self.get_all_lines_from_txt(out_file_path)
        expected = self.get_all_lines_from_txt(exp_file_path)
        # testing against single thread processing. Since the order of texts is not the same, the lengths are compared
        self.assertEqual(len(actual), len(expected))

    def test_corpus_to_bert_input_pipeline_clean_par(self):
        corp_path = self.ROOT_DIR + "\\data\\Horisont\\Hori\\horisont"
        out_file_path = self.ROOT_DIR + "\\data\\corp_res_clean_r_events_par.txt"
        exp_file_path = self.ROOT_DIR + "\\data\\corp_res_clean_r_events_exp.txt"
        clean_and_extract_parallel_corpus(corp_path, out_file_path, max_processes=8, clean=clean_med_r_events)
        actual = self.get_all_lines_from_txt(out_file_path)
        expected = self.get_all_lines_from_txt(exp_file_path)
        self.assertEqual(len(actual), len(expected))


if __name__ == '__main__':
    unittest.main()
