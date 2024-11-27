#
#   Picks randomly 1000 sentences for each target inflection types (1, 16, 17, 19) 
#   from the set of all extracted sentences containing words with homonymous forms 
#   ( `all_enc_koond_homonymous_forms_sentences.jl` ). 
#   
#   Saves resulting sentences in JSON format into files which names follow the 
#   pattern: 
#       f'infl_type_{infl_type}_randomly_picked_1000_sentences.jl'
#   By default, attempts to pick unique sentences across all inflection types. 
#
#   Prints statistics about the whole dataset and about randomly picked subsets. 
#  

import json
import os, os.path

from collections import defaultdict

import pandas as pd

from random import randint, seed

from tqdm import tqdm

PICK_TARGET = 1000

all_sentences_uniq = True   # If all sentences across all inflection types should be uniq

#input_forms_file = "homonymous_forms_16_17_19.csv"
input_forms_file = "homonymous_forms_1_16_17_19.csv"
assert os.path.exists(input_forms_file), f'(!) Missing homonymous forms file {input_file!r}'

input_sent_file = 'all_enc_koond_homonymous_forms_sentences.jl'
assert os.path.exists(input_sent_file), f'(!) Missing homonymous form sentences file {input_sent_file!r}'

def count_and_percent(items, items_total):
    assert items_total > 0
    return f'{items} / {items_total} ({(items/items_total)*100.0:.2f}%)'

df_homonymous_forms = pd.read_csv(input_forms_file, header=0, keep_default_na=False).to_dict(orient='split')
possible_homonymous_forms_by_infl_type = {}
fields = [ 'lemma', 'inflect_type', 'pos', 'case', 'form' ]
assert fields == df_homonymous_forms['columns']
lemma_is_form = 0
lemma_not_form = 0
for row in df_homonymous_forms['data']:
    row_items = { f:v for f,v in zip(fields, row) }
    pos   = row_items['pos']
    case  = row_items['case']
    form  = row_items['form']
    lemma = row_items['lemma']
    if form == lemma:
        lemma_is_form += 1
    else:
        lemma_not_form += 1
    origin_inf_type = row_items['inflect_type']
    inf_type = origin_inf_type
    if inf_type == '19|2':
        inf_type = '19'
    if inf_type not in possible_homonymous_forms_by_infl_type.keys():
        possible_homonymous_forms_by_infl_type[inf_type] = {}
    if form not in possible_homonymous_forms_by_infl_type[inf_type]:
        possible_homonymous_forms_by_infl_type[inf_type][form] = []
    possible_homonymous_forms_by_infl_type[inf_type][form].append( {'pos':pos, 'case': case, 'inf_type': origin_inf_type} )

print( 'Total homonymous forms: ', lemma_is_form+lemma_not_form )
print()
print( '     homonymous forms   ' )
print( '    with lemma == form: ', lemma_is_form )
print()
for infl_type in sorted(possible_homonymous_forms_by_infl_type.keys(), key=lambda x: len(possible_homonymous_forms_by_infl_type[x]), reverse=True):
    print( f'   INFLECTION TYPE {infl_type}:  homonymous word forms : ', len(possible_homonymous_forms_by_infl_type[infl_type]) )
print()

homonymous_form_sents_by_infl_type = {}
homonymous_form_uniq_sents_by_infl_type = {}
homonymous_form_uniq_words_by_infl_type = {}
sent_id = 0
with open(input_sent_file, 'r', encoding='utf-8') as in_f:
    for line in tqdm(in_f, ascii=True):
        line = line.strip()
        if len(line) > 0:
            sent_id += 1
            sent_json = json.loads(line)
            assert 'text' in sent_json.keys()
            assert 'forms' in sent_json.keys()
            sent_str = sent_json['text']
            common_noun_forms = []
            for form_entry in sent_json['forms']:
                word_text  = form_entry['word']
                word_form  = form_entry['form']
                word_pos   = word_form.split('_')[0]
                word_feats = word_form.split('_')[1]
                start, end = form_entry['start'], form_entry['end']
                word_in_sent = sent_str[start:end]
                assert word_in_sent.lower() == word_text.lower()
                for infl_type in sorted(possible_homonymous_forms_by_infl_type.keys()):
                    if word_text in possible_homonymous_forms_by_infl_type[infl_type].keys():
                        for entry in possible_homonymous_forms_by_infl_type[infl_type][word_text]:
                            if entry['pos'] == word_pos:
                                if infl_type not in homonymous_form_sents_by_infl_type:
                                    homonymous_form_sents_by_infl_type[infl_type] = []
                                    homonymous_form_uniq_sents_by_infl_type[infl_type] = set()
                                    homonymous_form_uniq_words_by_infl_type[infl_type] = set()
                                new_entry = {}
                                new_entry["corpus"] = sent_json["corpus"]
                                new_entry["doc_id"] = sent_json["doc_id"]
                                new_entry["sent_id"] = sent_id
                                new_entry["text"]  = sent_json["text"]
                                new_entry["word"]  = word_text
                                new_entry["partofspeech"] = word_pos
                                new_entry["start"] = form_entry['start']
                                new_entry["end"]   = form_entry['end']
                                homonymous_form_sents_by_infl_type[infl_type].append(new_entry)
                                homonymous_form_uniq_sents_by_infl_type[infl_type].add(sent_id)
                                homonymous_form_uniq_words_by_infl_type[infl_type].add(word_text)
                                #print( new_entry )
                                break

print()
for infl_type in sorted(homonymous_form_sents_by_infl_type.keys(), key=lambda x: len(homonymous_form_sents_by_infl_type[x]), reverse=True):
    uniq_sentences = len(homonymous_form_uniq_sents_by_infl_type[infl_type])
    propernoun_sentences = len([s for s in homonymous_form_sents_by_infl_type[infl_type] if s["partofspeech"] == 'H'])
    total_sentences = len(homonymous_form_sents_by_infl_type[infl_type])
    print( f'    INFLECTION TYPE {infl_type}: number of homonymous form sentences: ', total_sentences)
    print( f'                           uniq sentences: ', count_and_percent(uniq_sentences, total_sentences))
    print( f'                propernoun word sentences: ', count_and_percent(propernoun_sentences, total_sentences))
    uniq_words = len(homonymous_form_uniq_words_by_infl_type[infl_type])
    all_words  = len(possible_homonymous_forms_by_infl_type[infl_type])
    print( f'all uniq words coverage (from VM lexicon): ', count_and_percent(uniq_words, all_words))

print()

# =====================   Pick N sentences randomly from each inflection types  ===============================
seed( 1 )
picked_sentences_by_type = {}
all_picked_sent_ids = set()
print(f'Picking randomly sentences from each inflection type ...')
print()
for infl_type in sorted(homonymous_form_sents_by_infl_type.keys(), key=lambda x: len(homonymous_form_sents_by_infl_type[x]), reverse=True):
    # =====================   Make a random pick  ===============================
    local_picked_sent_ids = set()
    picked_sentences_by_type[infl_type] = []
    _total = len(homonymous_form_sents_by_infl_type[infl_type])
    failed_attempts = 0
    total_sentences = len(homonymous_form_sents_by_infl_type[infl_type])
    print( f'   INFLECTION TYPE {infl_type}: picking {PICK_TARGET} sentences from total of {total_sentences} sentences ... ')
    while len( picked_sentences_by_type[infl_type] ) < PICK_TARGET:
        if _total > 1:
            i = randint(0, _total - 1)
        else:
            i = 0
        pick_sent = homonymous_form_sents_by_infl_type[infl_type][i]
        pick_sent_id = pick_sent['sent_id']
        sent_words = pick_sent['text'].split()
        all_sentences_uniq_satisfied = (not all_sentences_uniq) or \
            (all_sentences_uniq and pick_sent_id not in all_picked_sent_ids)
        local_picked_uniq_sentences_satisfied = \
            (pick_sent_id not in local_picked_sent_ids)
        # Sentence should be at least 4 words long and should 
        # contain at leaste one lowercase word and should be 
        # shorter than 1500 characters
        sent_constraints_satisfied = len(sent_words) >= 4 and \
            any( [w[0].islower() for w in sent_words] ) and \
            len(pick_sent['text']) < 1500
        if all_sentences_uniq_satisfied and \
           local_picked_uniq_sentences_satisfied and \
           sent_constraints_satisfied:
            picked_sentences_by_type[infl_type].append( pick_sent )
            failed_attempts = 0
            local_picked_sent_ids.add(pick_sent_id)
            all_picked_sent_ids.add(pick_sent_id)
        else:
            failed_attempts += 1
            if failed_attempts >= 20:
                print('(!) 20 unsuccessful random picks in a row: terminating ...')
                break
    print()
    # =====================   Get summary statistics of picked sentences  ===============================
    p_uniq_sentences_by_id   = len(set([e["sent_id"] for e in picked_sentences_by_type[infl_type] ]))
    p_uniq_sentences_by_text = len(set([e["text"] for e in picked_sentences_by_type[infl_type] ]))
    p_uniq_words = len(set([e["word"] for e in picked_sentences_by_type[infl_type] ]))
    p_propernoun_sentences = \
        len([s for s in picked_sentences_by_type[infl_type] if s["partofspeech"] == 'H'])
    total_sentences2 = len(picked_sentences_by_type[infl_type])
    print( f'         picked uniq sentences (by sent_id): ', count_and_percent(p_uniq_sentences_by_id, total_sentences2))
    print( f'       picked uniq sentences (by sent_text): ', count_and_percent(p_uniq_sentences_by_text, total_sentences2))
    print( f'                  propernoun word sentences: ', count_and_percent(p_propernoun_sentences, total_sentences2))
    all_words  = len(possible_homonymous_forms_by_infl_type[infl_type])
    print( f'   all uniq words coverage (from VM lexicon): ', count_and_percent(p_uniq_words, all_words))
    print()
    out_file = f'infl_type_{int(infl_type):02d}_randomly_picked_{PICK_TARGET}_sentences.jl'
    with open( out_file, 'w', encoding='utf-8' ) as out_f:
        for entry_dict in picked_sentences_by_type[infl_type]:
            out_f.write( json.dumps(entry_dict, ensure_ascii=False) )
            out_f.write( '\n' )
    print( f'   Writing randomly picked sentences into file {out_file} ... ')
    print()
    if True:
        # debug: display avg sentence length, shortest sentences, and longest sentence lengths (+ on example)
        sorted_txts = sorted([s["text"] for s in picked_sentences_by_type[infl_type]], key=lambda x: len(x))
        from statistics import mean
        print( 'mean sentence length: ', mean( [len(s) for s in sorted_txts] ) )
        print( '3 longest lengths:    ', [len(s) for s in sorted_txts][-5:] ) 
        for s in sorted_txts:
            if len(s) > 1400:
                print( f'sentence with length > 1400 ({len(s)}):    {s!r}' )
                break
        print( '3 shortest sentences: ', [f'{s!r}({len(s)})' for s in sorted_txts[:3]])
        print()
print()

