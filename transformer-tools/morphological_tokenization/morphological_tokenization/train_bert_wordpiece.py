#!/usr/bin/env python3

"""Treenimine
"""

import glob
import sys
from typing import Dict, List, Union, Type  # , Iterator, Optional

from tokenizers import BertWordPieceTokenizer


class MYTRAINER:
    """Treenimine"""

    def __init__(self, vocab_in, weight: int) -> None:
        pre_vocab: Union[None, Dict[str, int]] = None
        if vocab_in is not None:
            tmp_list: List[str] = vocab_in.read().splitlines() + [
                "[PAD]",
                "[UNK]",
                "[CLS]",
                "[SEP]",
                "[MASK]",
            ]
            pre_vocab: Dict[str, int] = {x: weight for x in tmp_list}

        # Initialize an empty tokenizer
        self.tokenizer: Type[BertWordPieceTokenizer] = BertWordPieceTokenizer(
            vocab=pre_vocab,
            clean_text=True,
            handle_chinese_chars=False,  # True
            strip_accents=False,  # True
            lowercase=True,
        )

    def train(self, files_in: List[str], vsize: int) -> None:
        """Train

        Args:
            files (List[str]): Sisendfailide nimed
            vsize (int): Väljundleksikoni (max) suurus
        """
        self.tokenizer.train(
            files_in,
            vocab_size=vsize,
            min_frequency=2,
            show_progress=True,
            special_tokens=["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"],
            limit_alphabet=1000,
            wordpieces_prefix="##",
        )

    def save(self, outdir: str, fname: str) -> None:
        """Save the files

        Args:
            outdir (str): väljundkataloog
            fname (str): väljundfaili nimi väljundkataloogis
        """
        self.tokenizer.save_model(outdir, fname)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--files",
        default=None,
        metavar="path",
        type=str,
        required=True,
        help="The files to use as training; accept '**/*.txt' type of patterns if enclosed in quotes",
    )
    parser.add_argument(
        "--vocab",
        default=None,
        type=argparse.FileType("r"),
        help="The name of the input vocab file",
    )
    parser.add_argument(
        "--out",
        default="./",
        type=str,
        help="Path to the output directory, where the files will be saved",
    )
    parser.add_argument(
        "--name",
        default="bert-wordpiece",
        type=str,
        help="The name of the output vocab file",
    )
    parser.add_argument(
        "--size", default=10000, type=int, help="The size of the output vocab"
    )
    parser.add_argument(
        "--weight", default=2**30, type=int, help="The weight used by input vocab"
    )
    args = parser.parse_args()

    files: List[str] = glob.glob(args.files)
    if not files:
        print(f"File does not exist: {args.files}")
        sys.exit(1)

    mt = MYTRAINER(args.vocab, args.weight)
    mt.train(files, args.size)
    mt.save(args.out, args.name)
