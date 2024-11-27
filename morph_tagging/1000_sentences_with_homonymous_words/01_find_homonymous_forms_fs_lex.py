#
#  Finds all word forms belonging to target inflection types (1, 16, 17, 19) based on 
#  inflection type information available in Vabamorf's lexicon, generates all homonymous 
#  word forms of target types and saves into file `homonymous_forms_1_16_17_19.csv`
#
#  As a Vabamorf's lexicon, we use Sander Saska's inflectional type detection dataset 
#  `andmed_algvormidega.csv`, which has been automatically derived from Vabamorf's 
#  lexicon `fs_lex` (can be obtained from here: 
#  https://github.com/SanderSaska/MorfoloogiliseMuuttyybiAutomaatneTuvastaja). 
#


import json
import os, os.path

import pandas as pd

from estnltk import Text
from estnltk.taggers import VabamorfAnalyzer

target_infl_types = ['1', '16', '17', '19']

vm_analyzer = VabamorfAnalyzer()

def is_compound_word( lemma:str ):
    analyses = vm_analyzer.analyze_token(lemma)
    compounds = []
    for a in analyses:
        compounds.append( len(a['root_tokens']) > 1 )
    if any(compounds) and not all(compounds):
        pass
        #print(f'(!) ambigious compound: {lemma!r} ')
        #print(f'(!) {analyses!r} ')
        #print()
    return any(compounds)

input_file = "andmed_algvormidega.csv"
assert os.path.exists(input_file), \
    f'(!) Missing file {input_file!r}. Get it from here: https://github.com/SanderSaska/MorfoloogiliseMuuttyybiAutomaatneTuvastaja'

df_algvormidega = pd.read_csv(input_file, header=0, keep_default_na=False).to_dict(orient='split')
c = 0
all_lemmas = set()
lemmas_by_inflect_type = dict()
for row in df_algvormidega['data']:
    [word, postags_str, inflection_type_str, lemma] = row
    postags = eval(postags_str)
    inflection_types = eval(inflection_type_str)
    postags_norm = '|'.join(sorted(list(set(postags))))
    if lemma not in all_lemmas:
        if not is_compound_word( lemma ):
            for inf_type in inflection_types:
                if inf_type in target_infl_types:
                    if inflection_type_str not in lemmas_by_inflect_type.keys():
                        lemmas_by_inflect_type[inflection_type_str] = []
                    lemmas_by_inflect_type[inflection_type_str].append((lemma, postags_norm))
        all_lemmas.add(lemma)
    c += 1
    #if c > 50:
    #    break

cases_noun = [
  # label, Estonian case name
  ('n', 'nimetav'),
  ('g', 'omastav'),
  ('p', 'osastav'),
  ('adt', 'lühike sisseütlev'),
  ('ill', 'sisseütlev'),
  ('in', 'seesütlev'),
  ('el', 'seestütlev'),
  ('all', 'alaleütlev'),
  ('ad', 'alalütlev'),
  ('abl', 'alaltütlev'),
  ('tr', 'saav'),
  ('ter', 'rajav'),
  ('es', 'olev'),
  ('ab', 'ilmaütlev'),
  ('kom', 'kaasaütlev')]

from estnltk.vabamorf.morf import Vabamorf as VabamorfInstance
# Use an "no speller" lexicon directory instead of the default one
nosp_vm_instance = VabamorfInstance( lexicon_dir='2020-01-22_nosp' )

def synthesize_all_nominal_forms(word, PoS):
    if len(PoS) > 1:
        print(f'(!) shortening pos {PoS!r} for {word!r}')
        PoS = PoS[0]
    rows = []
    for cas, case_name_est in cases_noun:
        if cas != 'adt':
            for form in nosp_vm_instance.synthesize(word, 'sg ' + cas, partofspeech=PoS):
                rows.append( {'lemma':word, 'pos':PoS, 'case': 'sg ' + cas, 'form': form} )
            for form in nosp_vm_instance.synthesize(word, 'pl ' + cas, partofspeech=PoS):
                rows.append( {'lemma':word, 'pos':PoS, 'case': 'pl ' + cas, 'form': form} )
        else:
            for form in nosp_vm_instance.synthesize(word, cas, partofspeech=PoS):
                rows.append( {'lemma':word, 'pos':PoS, 'case': 'sg ill', 'form': form} )
    return rows

def find_all_homonymous_forms(lemma, PoS):
    all_forms = synthesize_all_nominal_forms(lemma, PoS)
    homonymous_forms = []
    homonymous_form_ids = set()
    for rid1, row1 in enumerate(all_forms):
        for rid2, row2 in enumerate(all_forms):
            if row1['case'] != row2['case']:
                if row1['form'] == row2['form']:
                    if rid1 not in homonymous_form_ids:
                        homonymous_forms.append(row1)
                        homonymous_form_ids.add(rid1)
                    if rid2 not in homonymous_form_ids:
                        homonymous_forms.append(row2)
                        homonymous_form_ids.add(rid2)
    return homonymous_forms

def bulk_find_all_homonymous_forms(lemmas_postags):
    all_homonymous_forms = []
    for (lemma, postags_norm) in lemmas_postags:
        homonymous_forms = find_all_homonymous_forms(lemma, postags_norm)
        all_homonymous_forms.extend(homonymous_forms)
    return all_homonymous_forms

collected_all_homonymous_forms = []
for inf_type in sorted(lemmas_by_inflect_type.keys()):
    print(inf_type)
    print( ' ', len(lemmas_by_inflect_type[inf_type]) )
    all_homonymous_forms = \
        bulk_find_all_homonymous_forms( lemmas_by_inflect_type[inf_type] )
    print( ' ', all_homonymous_forms[:15] )
    for hf in all_homonymous_forms:
        hf['inflect_type'] = '|'.join(eval(inf_type))
        collected_all_homonymous_forms.append(hf)
    print() 

fields = ['lemma', 'inflect_type', 'pos', 'case', 'form']
with open(f'homonymous_forms_{"_".join(target_infl_types)}.csv', mode='w', encoding='utf-8') as out_f:
    out_f.write(','.join(fields))
    out_f.write('\n')
    for hf in collected_all_homonymous_forms:
        for f in fields:
            out_f.write( hf[f] )
            if f != fields[-1]:
                out_f.write(',')
            else:
                out_f.write('\n')
