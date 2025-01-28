#!/usr/bin/env python3

"""Tokeniseerime kasutades varemtehtud `vocabulary`
"""

import sys
#from typing import Dict, Iterator, List, Optional, Union, Type
from tokenizers import Tokenizer, models, pre_tokenizers, decoders  # , trainers

vocab_txt: str = (
    "../training/dataset_training_bigger/vocab4bert/text_bert_wordpiece-vocab.txt"
)

class MYTOKENIZE:
    """Tokenisaator"""

    def __init__(self, vocab: str) -> None:
        """Load your WordPiece lexicon

        Args:
            vocab (str): leksikon
        """
        self.tokenizer = Tokenizer(models.WordPiece.from_file(vocab))

    def tokenize(self, finput, foutput) -> None:
        """Initialize the pre-tokenizer and decoder then Tokenize text

        Args:
            input (_type_): Tokeniseeritav fail
            output (_type_): Tokeniseerimise tulemus sellesse faili
        """
        # Initialize the pre-tokenizer and decoder
        self.tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()
        self.tokenizer.decoder = decoders.WordPiece()

        # Tokenize text
        text: str = finput.read().lower()
        result = self.tokenizer.encode(text)

        # The output tokens
        foutput.write(result.tokens[0])
        for token in result.tokens[1:]:
            foutput.write(f' {token}' if token.startswith('##') else f'\n{token}')
        foutput.write('\n')

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--vocab", required=True, type=str, help="The name of the input vocab file"
    )
    parser.add_argument(
        "--input",
        default=sys.stdin,
        type=argparse.FileType("r"),
        help="The name of the input filename",
    )
    parser.add_argument(
        "--output",
        default=sys.stdout,
        type=argparse.FileType("w"),
        help="The name of the output filename",
    )
    args = parser.parse_args()

    tkn = MYTOKENIZE(args.vocab)
    tkn.tokenize(args.input, args.output)
