#!/usr/bin/env python3

"""Tekstikorpusest sagedusloendi tegemine.
Kusutame HTML märgendid ja tõstame sõne külge kleepunud märgid lahku.

Read: `sagedus` `sõne`


```cmdline
export workspaceFolder=~/git/huggingface_github/estnltk_model_training_github
cd ${workspaceFolder}/transformer-tools/morphological_tokenization/
venv/bin/python3 remove_html.py \
    --in_html=../training/dataset_training_bigger/ettenten_0.fshtml \
    --out_txt=../training/dataset_training_bigger/ettenten_0.txt
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
import sys
import re
from typing import Type, Dict, List #  Iterator, Optional, Union
import string
from tqdm import tqdm

# https://op.europa.eu/en/web/eu-vocabularies/formex/physical-specifications/character-encoding/quotation-marks
quotation_marks: str = "\"'«»‘’‚‛“”„‟‹›"

class RemoveHTML:
    """Klass sisendteksti sõnestamiseks ja sagedusloendi tegemiseks
    """
    def __init__(self) -> None:
        """Algväärtustame muutujad
        """
        self.garbage_around_the_token: str = string.punctuation + "\"'«»‘’‚‛“”„‟‹›"

    def remove(self, fn_in: str, fn_out: str) -> None:
        """Sisendtekst sõnedeks.

        Kusutame HTML märgendid ja sõnade ümbert jutumärgid, sulud jms punktuatsiooni.
        Teisendame kõik sõnad väiketäheliseks.

        Args:
            filename (str): Sisendteksti sisaldava faili nimi

        Out:
            self.tokens (List[str]): Tekstisõned
        """
        with open(fn_in, "r", encoding="utf-8") as in_file:
            print(f'# {fn_in} --> {fn_out}')
            in_text_clean: str = re.sub('<.*?>', ' ',  in_file.read())
            with open(fn_out, "w") as out_file:
                out_file.write(in_text_clean)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--in_html",
        default="",
        type=str,
        help="Input filename",
    )
    parser.add_argument(
        "--out_txt",
        default="",
        type=str,
        help="Output filename",
    )
    args = parser.parse_args()

    RemoveHTML().remove(args.in_html, args.out_txt)
