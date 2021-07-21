import csv
import os
import sys
import time

from estnltk import Text

from .textprocessing.text_cleaning import reformat_sentences, clean_med_r_events, clean_med


def clean_and_extract_sentences_tsv(in_tsv_path, out_path, text_col_i=1, clean=None):
    """
        A pipeline that converts and cleans text from a source tsv file into usable form for training a BERT model.
        :param in_tsv_path: path to the source .tsv file
        :param out_path: path to the output .txt file
        :param text_col_i: index of the column, that contains text to be cleaned and extracted
        :param clean: Function, that cleans takes an EstNLTK object as an argument and cleans it.
        There are two pre-made cleaning functions in this package {clean_med, clean_med_r_events}
        """

    text_col_i = int(text_col_i)

    if clean == "clean_med":
        clean = clean_med

    elif clean == "clean_med_r_events":
        clean = clean_med_r_events

    file = open(in_tsv_path, newline='', encoding='utf-8')
    out_file = open(out_path, 'w', newline='', encoding='utf-8')
    tsv_reader = csv.reader(file, delimiter="\t")
    tsv_writer = csv.writer(out_file, delimiter="\t")

    for i, row in enumerate(tsv_reader):
        if i == 0:
            tsv_writer.writerow([row[text_col_i]])
            continue

        if len(row) < text_col_i:
            continue

        text_obj = Text(row[text_col_i])
        if clean is not None:
            text_obj = clean(text_obj)
        sentences = reformat_sentences(text_obj)
        tsv_writer.writerow([sentences])

    file.close()
    out_file.close()


if __name__ == "__main__":
    a = sys.argv[1:]
    time0 = time.time()
    clean_and_extract_sentences_tsv(*a)
    os.system("echo Time taken: " + str(time.time() - time0))
