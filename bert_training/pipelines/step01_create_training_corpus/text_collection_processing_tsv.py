import csv
import sys

from .textprocessing.text_cleaning import extract_segments, preprocess_to_estnltk_Text, clean_and_extract_sentences, \
    remove_beginning_symbols


def clean_and_extract_sentences_tsv(in_tsv_path, out_path):
    """
        A pipeline that converts and cleans text from a source tsv file into usable form for training a BERT model.
        :param in_tsv_path: path to the source .tsv file
        :param out_path: path to the output .txt file
        :return:
        """
    file = open(in_tsv_path, newline='', encoding='utf-8')
    outfile = open(out_path, 'w', encoding='utf-8')
    read_tsv = csv.reader(file, delimiter="\t")

    for i, row in enumerate(read_tsv):
        if i == 0:
            continue

        if (len(row) != 3):
            continue

        text = row[1]
        texts = extract_segments(text)
        for text in texts:
            t = preprocess_to_estnltk_Text(text)
            sentences = clean_and_extract_sentences(t)
            r = ""
            for s in sentences:
                r += remove_beginning_symbols(s) + "\n"
            r += "\n"
            outfile.write(r)

    file.close()
    outfile.close()


if __name__ == "__main__":
    a = sys.argv[1:]
    clean_and_extract_sentences_tsv(*a)
