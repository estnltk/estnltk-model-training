#
#  Concatenates cut_sentences_*.conllu files with original 
#  et_edt-ud-*-morph_extended.conllu files and creates a new 
#  file. Takes 3 input parameters:
#    INPUT_A.conllu  INPUT_B.conllu  OUTPUT_C.conllu
#
#  Usage examples:
# 
#  python  02x_concat_conllu_files.py  et_edt-ud-train-morph_extended.conllu  \
#          cut_sentences_train.conllu  et_edt-ud-train-with-cut-train-morph_extended.conllu
#  python  02x_concat_conllu_files.py  et_edt-ud-dev-morph_extended.conllu  \
#          cut_sentences_dev.conllu  et_edt-ud-dev-with-cut-train-morph_extended.conllu
#
#

from collections import defaultdict
import os, os.path, sys

# Validate that the result is a valid conllu file 
# that stanza can load (requires stanza package)
validate_conllu = True

input_files = []
output_file = None

for arg in sys.argv:
    if arg.endswith('.conllu'):
        if len(input_files) < 2:
            assert os.path.isfile(arg), \
                f'(!) Bad or non-existent input file {arg!r}'
            input_files.append(arg)
        else:
            output_file = arg

if len(input_files) == 2 and output_file is not None:
    print(f' Concatenating {" & ".join(input_files)} into {output_file}.')
    conllu_content = []
    sent_ids = defaultdict(int)
    sentence_count = 0
    token_count = 0
    for fname in input_files:
        with open(fname, 'r', encoding='utf-8') as in_f:
            empty_lines_in_row = 0
            for line in in_f:
                line_clean = line.strip()
                if len(line_clean) > 0:
                    if line_clean[0].isnumeric():
                        if line_clean.startswith('1\t'):
                            sentence_count += 1
                        token_count += 1
                    elif line_clean.startswith('# sent_id = '):
                        sent_ids[line_clean] += 1
                    empty_lines_in_row = 0
                else:
                    empty_lines_in_row += 1
                if empty_lines_in_row != 2:
                    conllu_content.append(line)
    out_fname = f'{output_file}'
    with open(out_fname, 'w', encoding='utf-8') as out_f:
        for line in conllu_content:
            out_f.write( line )
    print(f' ->  {out_fname}')
    if validate_conllu:
        # Import stanza data loading functions
        from stanza.utils.conll import CoNLL
        from stanza.models.common.doc import Document
        from stanza.utils.conll18_ud_eval import load_conllu_file
        # 1) Test data loading for training (stanza version 1.9.2)
        train_data, train_comments, train_empty = \
            CoNLL.conll2dict(input_file=out_fname, ignore_gapping=True)
        train_doc = Document(train_data, 
                                comments=train_comments, 
                                    empty_sentences=train_empty)
        print( f'{out_fname!r} conllu loading test #1: OK' )
        # 2) Test data loading for evaluation
        treebank_type = {}
        treebank_type['no_gapping'] = 0
        treebank_type['no_shared_parents_in_coordination'] = 0
        treebank_type['no_shared_dependents_in_coordination'] = 0
        treebank_type['no_control'] = 0
        treebank_type['no_external_arguments_of_relative_clauses'] = 0
        treebank_type['no_case_info'] = 0
        treebank_type['no_empty_nodes'] = False
        treebank_type['multiple_roots_okay'] = False
        eval_doc = load_conllu_file(out_fname, treebank_type)
        print( f'{out_fname!r} conllu loading test #2: OK' )
