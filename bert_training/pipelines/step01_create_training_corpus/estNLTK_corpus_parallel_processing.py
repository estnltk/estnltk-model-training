import csv

from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpora

from .textprocessing.parallel_workers import clean_and_extract_parallel


def clean_and_extract_parallel_corpus(corpus_path, out_path, max_processes=3, clean=None):
    """
        A parallelized pipeline that converts and cleans text from a source tsv file into usable form for training a
         BERT model.
        :param tsv_path: path to the source .tsv file
        :param out_path: path to the output .txt file
        :param text_col_i: index of the column, that contains text to be cleaned and extracted
        :param max_processes: The number of processes you want to create and use. Note that you don't need as many if
         you use a cheap clean function (if at all) than an expensive one.
        :param clean: Function, that cleans takes an EstNLTK object as an argument and cleans it.
        There are two pre-made cleaning functions in this package {clean_med, clean_med_r_events}
        :param tsv_delimiter: default = ''
        :param tsv_encoding: default = utf-8
        :param tsv_newline: default = \t
        """
    clean_and_extract_parallel(_read_corpus, (corpus_path,), out_path, max_processes, clean)


def _read_corpus(recorded_data, corpus_path):
    """
    A function that reads texts from a tsv file and puts them into multiprocessing Queue "recorder_data"
    :param recorded_data: A queue shared by this and text processing threads
    :param corpus_path:
    """
    for text_obj in parse_tei_corpora(corpus_path, target=['artikkel']):
        recorded_data.put(text_obj)
