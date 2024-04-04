#!/usr/bin/python3

"""_summary_

```cmdline
export workspaceFolder=~/git/huggingface_github/estnltk_model_training_github
cd ${workspaceFolder}/transformer-tools/morphological_tokenization/
../training/venv/bin/python3 create_lexicon.py --max=1000 \
    ../training/dataset_training_bigger/tykid.tsv \
    > ../training/dataset_training_bigger/1000.lex
```

code:
```json
    {
        "name": "create_dct_1000",
        "type": "debugpy",
        "request": "launch",
        "cwd": "${workspaceFolder}/transformer-tools/morphological_tokenization/",
        "program": "./create_dct.py",
        "env": {},
        "args": ["--max=1000", "../training/dataset_training_bigger/tykid.tsv"]
    },
```

"""

import sys
#import subprocess
import argparse
#import traceback
import json
from typing import Dict, List #, Tuple

VERSION = "2024.03.4"

def create_dct(lines:List[str], max_size: int) -> Dict:
    """_summary_

    Args:
        lines (List[str]): Sisendfaili read:
        max_size (int): _description_

    Returns:
        Dict: _description_
    """
    dct = {}
    for line in lines[1:]:
        line = line.strip().split('\t')
        if int(line[6]) < max_size:
            dct[line[1]] = line[2].strip("[] ")
        else:
            break
    return dct

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(allow_abbrev=False)
    argparser.add_argument('-v', '--version', action="store_true", help='show version info')
    argparser.add_argument('-m', '--max', type=int, help='~ max tükkide arv')
    argparser.add_argument('file', type=argparse.FileType('r'), nargs='+')
    args = argparser.parse_args()

    if args.version is True:
        print("VERSION_split_tokens = ", VERSION)
        sys.exit(0)

    for file in args.file:
        json.dump(create_dct(file.readlines(), args.max), sys.stdout, ensure_ascii=False)
        sys.stdout.write('\n')
    sys.exit(0)
