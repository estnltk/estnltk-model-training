from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpora

from textprocessing.text_cleaning import clean_and_extract_sentences, \
    extract_sentences


def clean_and_extract_sentences_corpus(corp_path, out_file_path, clean=False):
    """
    Cleans (optinal) and extract sentences from a corpus.
    :param corp_path: path to the corpus directory that contains .xml files
    :param out_file_path: output .txt file
    :param clean: Boolean, Set True to also clean the texts using the methods designed for clinical notes
    :return:
    """
    with open(out_file_path, "w", encoding="utf-8") as out_file:
        for text_obj in parse_tei_corpora(corp_path, target=['artikkel']):
            if clean:
                sentences = clean_and_extract_sentences(text_obj)
            else:
                sentences = extract_sentences(text_obj)
            out_file.write("\n".join(sentences))
            out_file.write("\n")
