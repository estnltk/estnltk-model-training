import csv
import os
from os import listdir
from os.path import isfile, join

import pandas as pd

from textprocessing.text_cleaning import extract_segments, \
    preprocess_to_estnltk_Text, clean_and_extract_sentences, remove_beginning_symbols


def clean_and_extract_sentences_tsv(in_tsv_path, out_tsv_path):
    file = open(in_tsv_path, newline='', encoding='utf-8')
    outfile = open(out_tsv_path, 'w', newline='', encoding='utf-8')
    read_tsv = csv.reader(file, delimiter="\t")
    writer = csv.writer(outfile)

    for i, row in enumerate(read_tsv):
        if i == 0:
            writer.writerow(["epi_id", "text", "source"])
            continue

        if (len(row) != 3):
            continue

        epi_id = row[0]
        text = row[1]
        source = row[2]
        texts = extract_segments(text)
        for text in texts:
            t = preprocess_to_estnltk_Text(text)
            sentences = clean_and_extract_sentences(t)
            r = ""
            for s in sentences:
                r += remove_beginning_symbols(s) + "\n"
            r += "\n"
            writer.writerow([epi_id, r, source])

    file.close()
    outfile.close()


def clean_and_extract_sentences_tsv_par(in_tsv_path, out_tsv_path, temp_dir_path, shards=32, threads=32,
                                        make_shards=True):
    shard_dir = temp_dir_path + "shards/"
    cleaned_dir = temp_dir_path + "cleaned/"

    if make_shards:  # makes shards
        # loading the data
        df = pd.read_csv(in_tsv_path, sep='\t', header=0, error_bad_lines=False)

        # splitting the data
        S = len(df) // shards
        frames = [df.iloc[i * S:(i + 1) * S].copy() for i in range(shards)]

        # making a directory for shards

        os.system("mkdir " + shard_dir + " ")
        for i, frame in enumerate(frames):
            frame.to_csv(shard_dir + "shard_" + str(i) + ".tsv", sep="\t", index=False)

        os.system("echo splitting done! ")

    # getting tokens
    os.system("mkdir " + cleaned_dir + " ")
    XARGS_CMD = ("ls {} | "
                 "xargs -n 1 -P {} -I{} "
                 "python {}/text_collection_processing_tsv.py clean_and_extract_sentences_tsv {}{} {}{} ")

    XARGS_CMD = XARGS_CMD.format(shard_dir, threads, '{}', os.path.dirname(os.path.abspath(__file__)), shard_dir, '{}',
                                 cleaned_dir, '{}')
    os.system(XARGS_CMD)
    os.system("echo cleaned, now putting 1 big file back together! ")

    # combining the files
    files = [f for f in listdir(cleaned_dir) if isfile(join(cleaned_dir, f))]

    combined_csv = pd.concat([pd.read_csv(cleaned_dir + f, header=0) for f in files])
    combined_csv.to_csv(out_tsv_path, index=False, encoding='utf-8')

