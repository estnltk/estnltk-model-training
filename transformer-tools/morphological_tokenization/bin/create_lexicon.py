#!/usr/bin/python3

"""
code
    {
        "name": "create_lexicon 100",
        "type": "debugpy",
        "request": "launch",
        "cwd": "${workspaceFolder}/transformer-tools/morphological_tokenization/",
        "program": "./create_lexicon.py",
        "env": {},
        "args": ["--max=10000", "../training/dataset_training_bigger/lexicon_10000.txt"]
    },
"""

import sys
import argparse
import json
from typing import List  # Dict, Tuple

VERSION = "2024.03.4"


def create_lex(lines: List[str], max_size: int) -> List[str]:
    """Tee ettenatud suurusega leksikon

    Args:
        lines (List[str]): sagedusloend
        max_size (int): väljundleksikoni max suurus

    Returns:
        List[str]: leksikon
    """
    lex: List[str] = []
    for line in lines[1:]:
        line_splitted: List[str] = line.strip().split("\t")
        if int(line_splitted[6]) < max_size:
            # tüvedega majandamine
            stems: str = line_splitted[3].strip("[ ]")
            if len(stems) > 0:
                lex += stems.split(" ")
            # lõppudega majandamine
            endings: str = line_splitted[5].strip("[ ]")
            if len(endings) > 0:
                lex += endings.split(" ")
        else:
            break
    return lex


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(allow_abbrev=False)
    argparser.add_argument(
        "-v", "--version", action="store_true", help="show version info"
    )
    argparser.add_argument(
        "-j", "--json", action="store_true", help="True: JSON out; False: text out"
    )
    argparser.add_argument(
        "-i",
        "--indent",
        type=int,
        default=None,
        help="indent for json output, 0=all in one line",
    )
    argparser.add_argument("-n", "--n_tüve", type=int, help="~ tüvede arv")
    argparser.add_argument("-m", "--max", type=int, help="~ max tükkide arv")
    argparser.add_argument("file", type=argparse.FileType("r"), nargs="+")
    args = argparser.parse_args()

    if args.version is True:
        print("VERSION_split_tokens = ", VERSION)
        sys.exit(0)

    for file in args.file:
        if args.json is True:
            json.dump(
                create_lex(file.readlines(), args.max),
                sys.stdout,
                indent=args.indent,
                ensure_ascii=False,
            )
            sys.stdout.write("\n")
        else:
            for line in create_lex(file.readlines(), args.max):
                sys.stdout.write(f"{line}\n")
    sys.exit(0)
