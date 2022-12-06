#
#  Gathers final evalution statistics from log files and outputs in a concise view.
#

import os, os.path, sys
import re

_final_results_set_pat = re.compile(r'^Results of evaluating on (\S+) set\s*$')
_results_correct_pat   = re.compile(r'^\s*Words with correct annotations:\s+(\S+) / (\S+) \((\S+)\)\s*$')
_results_ambiguous_pat = re.compile(r'^\s*Words with ambiguous annotations:\s+(\S+) / (\S+) \((\S+)\)\s*$')

def fetch_final_statistics_from_eval_log( fname ):
    '''Reads final statistics from the given evaluation log file.'''
    assert os.path.isfile(fname)
    subset = None
    correct = None
    amb = None
    with open( fname, 'r', encoding='utf-8' ) as in_f:
        for line in in_f:
            line = line.rstrip()
            final_res = _final_results_set_pat.match(line)
            if final_res:
                subset = final_res.group(1)
            if subset is not None:
                correct_res = _results_correct_pat.match(line)
                if correct_res:
                    correct = ( correct_res.group(1),
                                correct_res.group(2),
                                correct_res.group(3) )
                amb_res = _results_ambiguous_pat.match(line)
                if amb_res:
                    amb = ( amb_res.group(1), amb_res.group(2),
                            amb_res.group(3) )
    if subset is None or correct is None or amb is None:
        raise ValueError( ('(!) Unable to fetch final evaluation results from file {!r}. '+\
                           'Is the file created with script 02_eval_ud_morph_conv.py ?').format(fname) )
    return subset, correct, amb

_fname_eval_log_pat = re.compile(r'^eval_(train|dev|test)_(\S+)_log_(\d\d\d\d\S+)\.(log|txt)$')

def parse_conf_and_datetime_from_fname( fname ):
    '''Parses subset, conf infix and datetime from given log file name.'''
    fname_match = _fname_eval_log_pat.match( fname )
    if fname_match:
        subset = fname_match.group(1)
        conf_infix = fname_match.group(2)
        datetime = fname_match.group(3)
        return subset, conf_infix, datetime
    else:
        raise ValueError( ('(!) Unable to parse evaluation file name {!r}. '+\
                           'Is the file created with script 02_eval_ud_morph_conv.py ?').format(fname) )

def _custom_res_sort_key( res_tuple ):
    subset_key = res_tuple[1]
    if subset_key == 'train':
        subset_key = 0
    elif subset_key == 'dev':
        subset_key = 1
    elif subset_key == 'test':
        subset_key = 2
    else:
        raise ValueError( '(!) Unexpected subset key: {!r}'.format( subset_key ) )
    return (res_tuple[0], subset_key, res_tuple[2])


if __name__ == '__main__':
    # Load statistics from log files 
    all_results = []
    for fname in os.listdir('.'):
        if _fname_eval_log_pat.match( fname ):
            subsetA, correct, amb = fetch_final_statistics_from_eval_log( fname )
            subsetB, conf_infix, datetime = parse_conf_and_datetime_from_fname( fname )
            assert subsetA == subsetB
            all_results.append( (conf_infix, subsetA, datetime, correct, amb) )
    # Sort and output results
    for res_tuple in sorted( all_results, key=_custom_res_sort_key ):
        res_string = f' {res_tuple[0]} | {res_tuple[1]:5} | {res_tuple[2]} | correct: {res_tuple[3][0]:>6}/{res_tuple[3][1]:>6} '+\
                     f'({res_tuple[3][2]:6}) | ambiguous: {res_tuple[4][0]:>6}/{res_tuple[4][1]:>6} ({res_tuple[4][2]:6})'
        print(res_string)


