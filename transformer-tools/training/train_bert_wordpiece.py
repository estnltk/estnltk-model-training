#!/usr/bin/env python3

import argparse
import glob
import sys
import os
from typing import Dict, Iterator, List, Optional, Union, Type

#sys.path.append(os.path.abspath('../bindings/python/py_src/tokenizers'))
from tokenizers import BertWordPieceTokenizer

parser = argparse.ArgumentParser()

parser.add_argument(
    "--files",
    default=None,
    metavar="path",
    type=str,
    required=True,
    help="The files to use as training; accept '**/*.txt' type of patterns \
                          if enclosed in quotes",
)

parser.add_argument(
    "--vocab",
    default=None,
    type=argparse.FileType('r'), 
)

parser.add_argument(
    "--out",
    default="./",
    type=str,
    help="Path to the output directory, where the files will be saved",
)
parser.add_argument("--name", default="bert-wordpiece", type=str, help="The name of the output vocab files")
args = parser.parse_args()

files = glob.glob(args.files)
if not files:
    print(f"File does not exist: {args.files}")
    exit(1)

pre_vocab: Union[None, Dict[str, int]] = None
if args.vocab is not None:
    tmp_list: List[str] = args.vocab.read().splitlines()
    pre_vocab = {x:10000 for x in tmp_list}

# Initialize an empty tokenizer
tokenizer: Type[BertWordPieceTokenizer] = BertWordPieceTokenizer(
    vocab=pre_vocab,
    clean_text=True,
    handle_chinese_chars=False, # True
    strip_accents=False,        # True
    lowercase=True,
)

# And then train
tokenizer.train(
    files,
    vocab_size=10000,
    min_frequency=2,
    show_progress=True,
    special_tokens=["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"],
    limit_alphabet=1000,
    wordpieces_prefix="##",
)

# Save the files
tokenizer.save_model(args.out, args.name)
