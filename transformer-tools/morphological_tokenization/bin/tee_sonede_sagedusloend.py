#!/usr/bin/env python3

"""Tekstikorpusest sagedusloendi tegemine.
Kusutame HTML märgendid ja tõstame sõne külge kleepunud märgid lahku.

Read: `sagedus` `sõne`


```cmdline
export workspaceFolder=~/git/huggingface_github/estnltk_model_training_github
cd ${workspaceFolder}/transformer-tools/morphological_tokenization/
venv/bin/python3 tee_sonede_sagedusloend.py \
    --files=../training/dataset_training_bigger/ettenten_0.txt \
    --out=../training/dataset_training_bigger/ettenten_0.freq
```

code
```json
        {
            "name": "sloend",
            "type": "debugpy",
            "request": "launch",
            "cwd":     "${workspaceFolder}/transformer-tools/training/",
            "program": "${workspaceFolder}/transformer-tools/training/tee_sonede_sagedusloend.py",
            "env": {},
            "args": ["--files=../training/dataset_training_bigger/ettenten_0.fshtml", `
            "--out=../training/dataset_training_bigger/ettenten_0.freq"]
        },
```
"""

import argparse
import glob
import sys
import re
from typing import Type, Dict, List #  Iterator, Optional, Union
import string
from tqdm import tqdm

# https://op.europa.eu/en/web/eu-vocabularies/formex/physical-specifications/character-encoding/quotation-marks
quotation_marks: str = "\"'«»‘’‚‛“”„‟‹›"

class FREQUENCIES:
    """Klass sisendteksti sõnestamiseks ja sagedusloendi tegemiseks
    """
    def __init__(self) -> None:
        """Algväärtustame muutujad
        """
        self.tokens: List[str] = []
        self.frequencies: Dict[str:int] = {}
        self.garbage_around_the_token: str = string.punctuation + "\"'«»‘’‚‛“”„‟‹›"

    def get_tokens(self, fn: str) -> None:
        """Sisendtekst sõnedeks.

        Kusutame HTML märgendid ja sõnade ümbert jutumärgid, sulud jms punktuatsiooni.
        Teisendame kõik sõnad väiketäheliseks.

        Args:
            filename (str): Sisendteksti sisaldava faili nimi

        Out:
            self.tokens (List[str]): Tekstisõned
        """
        with open(fn, "r", encoding="utf-8") as in_file:
            print(f'# Sisendfail: {fn}')
            #print('#    puhastame html-ist')
            in_text: str = in_file.read()
            #in_text_clean: str = re.sub('<.*?>', ' ', in_text)
            print('#    tükeldame white space-ide kohalt')
            self.tokens = in_text.split()
            pbar = tqdm(list(enumerate(self.tokens)), desc='#    kustutame punktuatsiooni')
            for idx, token in pbar:
                self.tokens[idx] = token.strip(self.garbage_around_the_token).lower()

    def get_frequencies(self) -> None:
        """Koostame sõnavormide sagedusloendi.

        In:
            self.tokens (List[str]): Tekstisõned

        Out:
            self.frequencies (Dict[str:int]): Tekstsõnede sagedusloend

        """
        print("\n# sõnede loend sagedustabeliks\n#    järjestame")
        self.tokens.sort()
        token_prev:str = self.tokens[0]
        count: int = 1
        pbar = tqdm(self.tokens[1:], desc='#    arvutame sagedused')
        for token in pbar:
            if token == token_prev:
                count += 1
            else:
                self.frequencies[token_prev] = count
                token_prev, count = token, 1
        self.frequencies[token_prev] = count
        self.frequencies = dict(sorted(self.frequencies.items(),
                                key=lambda item: item[1], reverse=True))

    def save_frequencies(self, fn:str) -> None:
        """Kuvame sageusloendi.

        Kuvame ainult tähtedest koosnevate tekstisõnede sagedusi.

        Args:
            fn (str): Väljundfaili nimi.
            
        In:
            self.frequencies (Dict[str:int]): Tekstsõnede sagedusloend

        Out:
            Failis `fn` read kujul: `sagedus` `sõne`
        """
        print(f"# tulemused faili {fn}")
        if fn == "":
            file_out = sys.stdout
        else:
            file_out = open(fn, "w", encoding="utf-8")
        for token, freq in  self.frequencies.items():
            if token.isalpha():
                print(f'{freq} {token}', file=file_out)

if __name__ == "__main__":
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
        "--out",
        default="",
        type=str,
        help="Output filename",
    )
    args = parser.parse_args()

    filenames: List[str] = glob.glob(args.files)
    if not filenames:
        print(f"File does not exist: {args.files}")
        sys.exit(1)
    sl: Type[FREQUENCIES] = FREQUENCIES()
    for filename in filenames:
        sl.get_tokens(filename)
    sl.get_frequencies()
    sl.save_frequencies(args.out)
