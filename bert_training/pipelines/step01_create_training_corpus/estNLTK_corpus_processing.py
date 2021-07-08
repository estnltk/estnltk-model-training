import sys

from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpora

from .textprocessing.text_cleaning import reformat_sentences


def clean_and_extract_sentences_corpus(corp_path, out_file_path, clean=None):
    """
    Cleans (optinal) and extract sentences from a corpus.
    :param corp_path: path to the corpus directory that contains .xml files
    :param out_file_path: output .txt file
    :param clean: Function, that cleans takes an EstNLTK object as an argument and cleans it.
    There are two pre-made cleaning functions in this package {clean_med, clean_med_r_events}
    """
    with open(out_file_path, "w", encoding="utf-8") as out_file:
        for text_obj in parse_tei_corpora(corp_path, target=['artikkel']):
            if clean is not None:
                text_obj = clean(text_obj)
            sentences = reformat_sentences(text_obj)
            out_file.write(sentences)
            out_file.write("\n\n")


if __name__ == "__main__":
    a = sys.argv[1:]
    clean_and_extract_sentences_corpus(*a)
