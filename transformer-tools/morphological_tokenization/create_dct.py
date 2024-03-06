 #!/usr/bin/python3

import sys
import subprocess
import argparse
import traceback
import json

'''
        {
            "name": "split_tokens",
            "type": "debugpy",
            "request": "launch",
            "cwd": "${workspaceFolder}",
            "program": "./split_tokens.py",
            "env": {},
            "args": ["test.txt"]
        }
'''

VERSION_split_tokens = "2024.02.15"

proc_split_tokens = subprocess.Popen(['./split_tokens', '--path', '.'], universal_newlines=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

def split_tokens(text_in:str) -> str:
    """Sõnede tükeldaja

    Tükeldame teksti 'white space'i kohalt, tõstame punktuatsiooni lahku ja
    tükeldame sõnad (tüve, liitsõnapiiri, liite, lõpi jms kohalt).

    Args:
        text_in (str): sisendtekst

    Returns:
        str: tükeldatud sisendtekst
    """
    try:    
        assert proc_split_tokens.stdin is not None and proc_split_tokens.stdout is not None
        text_in += '\n'
        proc_split_tokens.stdin.write(text_in)
        proc_split_tokens.stdin.flush()
        return proc_split_tokens.stdout.readline().strip()
    except Exception:
        traceback.print_exc()

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(allow_abbrev=False)
    argparser.add_argument('-v', '--version', action="store_true", help='show version info')
    argparser.add_argument('file', type=argparse.FileType('r'), nargs='+')
    args = argparser.parse_args()
    
    if args.version is True:
        print("VERSION_split_tokens = ", VERSION_split_tokens)
        sys.exit(0)

    for file in args.file:
        sys.stdout.write(split_tokens(file.read())+'\n')
    sys.exit(0)


    
