#!/usr/bin/python3

import sys
import subprocess
import argparse
import traceback
import json
from typing import Dict, List, Tuple
'''
code
    {
        "name": "create_dct.py",
        "type": "debugpy",
        "request": "launch",
        "cwd": "${workspaceFolder}/transformer-tools/morphological_tokenization/",
        "program": "./create_dct.py",
        "env": {},
        "args": ["--max=100", "--indent=4", "tykid.tsv"]
    },
'''

VERSION = "2024.03.4"

def create_dct(lines:List, max:int) -> Dict:
    dct = {}
    for line in lines[1:]:
        line = line.strip().split('\t')
        if int(line[6]) < max:
            dct[line[1]] = line[2].strip("[] ")
        else:
            break
    return dct

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(allow_abbrev=False)
    argparser.add_argument('-v', '--version', action="store_true", help='show version info')
    argparser.add_argument('-i', '--indent', type=int, default=None, help='indent for json output, 0=all in one line')
    argparser.add_argument('-n', '--n_tüve', type=int, help='~ tüvede arv')
    argparser.add_argument('-m', '--max', type=int, help='~ max tükkide arv')
    argparser.add_argument('file', type=argparse.FileType('r'), nargs='+')
    args = argparser.parse_args()
    
    if args.version is True:
        print("VERSION_split_tokens = ", VERSION)
        sys.exit(0)

    for file in args.file:
        json.dump(create_dct(file.readlines(), args.max), sys.stdout, indent=args.indent, ensure_ascii=False)
        sys.stdout.write('\n')
    sys.exit(0)


    
