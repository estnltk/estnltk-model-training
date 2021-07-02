import os
import sys
from os import listdir
from os.path import isfile, join

import pandas as pd


def clean_and_extract_sentences_tsv_par(in_tsv_path, out_txt_path, temp_dir_path, shards=32, threads=32,
                                        make_shards=True):
    """
    A parallelized pipeline that converts and cleans text from a source tsv file into usable form for training a BERT model.
    :param in_tsv_path: path to the source .tsv file
    :param out_txt_path: path to the output .tsv file
    :param temp_dir_path: the parallelization process splits the source tsv file into shards. The shards will be stored
     in this temp directory under <temp_dir_path>/shards. Also <temp_dir_path>/cleaned is created to store cleaned shards.
     After cleaning, the cleaned shards are put back together. However these directories wont be cleaned automatically
    :param shards: The number of shards you'd like to make
    :param threads: The number of threads you are using. Note: should be equal or lower than the number of shards.
    :param make_shards: Set True to remake the shards. Should be disabled if shards are already created and you
     don't want to overwrite them.
    :return:
    """

    shards = int(shards)
    threads = int(threads)
    make_shards = bool(make_shards)
    if shards < threads:
        threads = shards

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
                 "python {}/text_collection_processing_tsv.py {}{} {}{}{} ")

    XARGS_CMD = XARGS_CMD.format(shard_dir, threads, '{}', os.path.dirname(os.path.abspath(__file__)), shard_dir, '{}',
                                 cleaned_dir, '{}', '.txt')
    os.system(XARGS_CMD)
    os.system("echo cleaned, now putting 1 big file back together! ")

    # combining the files
    files = [f for f in listdir(cleaned_dir) if isfile(join(cleaned_dir, f))]

    with open(out_txt_path, 'w', encoding="utf-8") as outfile:
        for fname in files:
            with open(cleaned_dir + fname) as infile:
                for line in infile:
                    outfile.write(line)


if __name__ == "__main__":
    a = sys.argv[1:]
    clean_and_extract_sentences_tsv_par(*a)
