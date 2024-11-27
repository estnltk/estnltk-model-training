#
#  Extracts all sentences containing homonymous word forms (as 
#  listed in `homonymous_forms_1_16_17_19.csv`) from the vert files 
#  nc19_Balanced_Corpus.vert and nc19_Reference_Corpus.vert. 
#  
#  Creates 2 files:
#  
#  * `all_enc_koond_homonymous_forms_counts.jl` -- a file containing 
#     count statistics of each individual homonymous word form. For 
#     instance:
#         {"word": "kilu", 
#          "total": 437, 
#          "corpus_forms": [["S_sg n", 415], ["S_sg g", 20], ["S_sg p", 2]], 
#          "missing_forms": []}
#
#  * `all_enc_koond_homonymous_forms_sentences.jl` -- a file containing  
#     sentences with homonymous word forms. Note that one sentence can contain
#     more than one homonymous word forms. An example:
#         {"corpus": "nc19_Balanced_Corpus.vert", 
#          "doc_id": 13, 
#          "text": "Püügilimiit, mille piires võivad Eesti kalurid käesoleval aastal püüda on kokku 41 200 t kilu ja 39 000 t räime.", 
#          "forms": [{"word": "Eesti", "form": "H_sg g", "start": 33, "end": 38}, 
#                    {"word": "kilu", "form": "S_sg n", "start": 89, "end": 93}]}
#

import json
import os, os.path

import pandas as pd

from collections import defaultdict

from estnltk import Text
from estnltk.corpus_processing.parse_enc import parse_enc_file_iterator

#input_file = "homonymous_forms_16_17_19.csv"
input_file = "homonymous_forms_1_16_17_19.csv"
assert os.path.exists(input_file), f'(!) Missing homonymous forms file {input_file!r}'

output_sent_file = 'all_enc_homonymous_forms_sentences.jl'
output_stats_file = 'all_enc_homonymous_forms_counts.jl'

input_vert_files = []
#input_vert_files.append('../nc23_nc19_Balanced_Corpus.vert')
#input_vert_files.append('../nc23_Literature_Old.vert')
input_vert_files.append('nc19_Balanced_Corpus.vert')
input_vert_files.append('nc19_Reference_Corpus.vert')
assert all(os.path.exists(fpath) for fpath in input_vert_files), f'(!) Missing vert file(s): {input_vert_files!r}'

df_homonymous_forms = pd.read_csv(input_file, header=0, keep_default_na=False).to_dict(orient='split')
possible_homonymous_forms = {}
possible_homonymous_forms_by_pos = defaultdict(int)
fields = [ 'lemma', 'inflect_type', 'pos', 'case', 'form' ]
assert fields == df_homonymous_forms['columns']
for row in df_homonymous_forms['data']:
    row_items = { f:v for f,v in zip(fields, row) }
    pos   = row_items['pos']
    case  = row_items['case']
    form  = row_items['form']
    lemma = row_items['lemma']
    inf_type = row_items['inflect_type']
    if form not in possible_homonymous_forms:
        possible_homonymous_forms[form] = []
        possible_homonymous_forms_by_pos[pos] += 1
    possible_homonymous_forms[form].append( {'inf_type':inf_type, 'pos':pos, 'case': case} )

print( 'Total homonymous forms: ', len(possible_homonymous_forms.keys()) )
for pos in sorted(possible_homonymous_forms_by_pos.keys(), key=possible_homonymous_forms_by_pos.get, reverse=True):
    print( f'   homonymous forms with part-of-speech {pos}: ', possible_homonymous_forms_by_pos[pos] )

with open(output_sent_file, 'w', encoding='utf-8') as out_f:
    pass
with open(output_stats_file, 'w', encoding='utf-8') as out_f:
    pass

focus_block = None
focus_doc_ids = None
seen_sentences = set()
homonymous_form_sentences = []
homonymous_form_statistics = {}
for vert_file in input_vert_files:
    for text_obj in parse_enc_file_iterator( vert_file, line_progressbar='ascii', 
                                                        focus_block=focus_block, 
                                                        focus_doc_ids=focus_doc_ids, 
                                                        restore_morph_analysis=True, 
                                                        extended_morph_form=True,
                                                        add_document_index=True,
                                                        original_layer_prefix='' ):
        sentences_layer = text_obj['sentences']
        morph_layer = text_obj['morph_analysis']
        # Document id inside the vert file (not to be mistaken with 'id' in <doc> tag) 
        doc_file_id = text_obj.meta['_doc_id']
        for sentence in sentences_layer:
            found_homonymous_forms = []
            found_homonymous_form_cats = []
            found_homonymous_forms_locs = []
            for word in sentence:
                # Check (in a case insensitive manner) whether word is possibly homonymous
                matching_form = None
                if word.text in possible_homonymous_forms.keys():
                    matching_form = word.text
                elif (word.text).lower() in possible_homonymous_forms.keys():
                    matching_form = (word.text).lower()
                if matching_form is not None:
                    morph_span = morph_layer.get(word.base_span)
                    partofspeech_match = False
                    corpus_pos = morph_span.annotations[0]['partofspeech']
                    corpus_form = morph_span.annotations[0]['form']
                    for form_desc in possible_homonymous_forms[matching_form]:
                        if form_desc['pos'] == corpus_pos:
                            partofspeech_match = True
                    if partofspeech_match:
                        # Collect entry
                        found_homonymous_forms.append(matching_form)
                        found_homonymous_form_cats.append((corpus_pos, corpus_form))
                        found_homonymous_forms_locs.append((word.start-sentence.start, word.end-sentence.start))
                        # Record statistics
                        if matching_form not in homonymous_form_statistics:
                            homonymous_form_statistics[matching_form] = defaultdict(int)
                        homonymous_form_statistics[matching_form]['__total'] += 1
                        homonymous_form_statistics[matching_form][f'{corpus_pos}_{corpus_form}'] += 1
            if found_homonymous_forms:
                if sentence.enclosing_text not in seen_sentences:
                    _, vert_file_short = os.path.split(vert_file)
                    sent_entry = {'corpus': vert_file_short, \
                                  'doc_id': doc_file_id, \
                                  'text': sentence.enclosing_text, \
                                  'forms': [] }
                    for word, (start, end), (corpus_pos, corpus_form) in zip(found_homonymous_forms, \
                                                                             found_homonymous_forms_locs, \
                                                                             found_homonymous_form_cats):
                        # Validate word location
                        assert (sent_entry['text'][start:end].lower()) == word.lower()
                        # Add entry
                        sent_entry['forms'].append( { 'word':word, 'form': f'{corpus_pos}_{corpus_form}', \
                                                      'start':start, 'end': end } )
                    homonymous_form_sentences.append(sent_entry)
                    #print('>>', sent_entry)
                seen_sentences.add( sentence.enclosing_text )
        if len(homonymous_form_sentences) > 10000:
            with open(output_sent_file, 'a', encoding='utf-8') as out_f:
                for sent_entry in homonymous_form_sentences:
                    out_f.write( json.dumps(sent_entry, ensure_ascii=False) )
                    out_f.write( '\n' )
            homonymous_form_sentences = []
        #break


if len(homonymous_form_sentences) > 0:
    with open(output_sent_file, 'a', encoding='utf-8') as out_f:
        for sent_entry in homonymous_form_sentences:
            out_f.write( json.dumps(sent_entry, ensure_ascii=False) )
            out_f.write( '\n' )
    homonymous_form_sentences = []
with open(output_stats_file, 'a', encoding='utf-8') as out_f:
    for form in sorted( homonymous_form_statistics.keys(), key=lambda x:homonymous_form_statistics[x]['__total'], reverse=True ):
        entry = {'word': form, 
                 'total': homonymous_form_statistics[form]['__total'], 
                 'corpus_forms': [], 'missing_forms': []}
        corpus_pos_forms = []
        for pos_form in sorted(homonymous_form_statistics[form].keys(), key=lambda x:homonymous_form_statistics[form][x], reverse=True ):
            if pos_form != '__total':
                entry['corpus_forms'].append( [pos_form, homonymous_form_statistics[form][pos_form]] )
                corpus_pos_forms.append(pos_form)
        # check missing forms from all possible forms
        for possible_form in possible_homonymous_forms[form]:
            pos  = possible_form['pos']
            case = possible_form['case']
            if case == 'sg ill':
                case = 'adt'
            pos_case_short = f'{pos}_{case}'
            if pos_case_short not in corpus_pos_forms:
                entry['missing_forms'].append( pos_case_short )
        out_f.write( json.dumps(entry, ensure_ascii=False) )
        out_f.write( '\n' )
