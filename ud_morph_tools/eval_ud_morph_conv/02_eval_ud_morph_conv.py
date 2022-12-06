#
#  * Applies UDMorphConverter on specified corpus subsets and evaluates results;
#     * Loads evaluation configuration from an INI file (section [eval-settings]);
#     * Applies and evaluates UDMorphConverter on json input corpora; 
#     * Saves log file with detailed evaluation results (optional);
#

import os, os.path, sys
import configparser

from tqdm import tqdm
from datetime import datetime

from estnltk import Text
from estnltk.converters import json_to_text
from estnltk.taggers import UDMorphConverter

from ud_morph_comparator import UDMorphComparator

if __name__ == '__main__':
    # Parse configuration file
    config = configparser.ConfigParser()
    if len(sys.argv) < 2:
        raise Exception('(!) Missing input argument: name of the configuration INI file.')
    conf_file = sys.argv[1]
    if not os.path.exists(conf_file):
        raise FileNotFoundError("Config file {} does not exist".format(conf_file))
    if len(config.read(conf_file)) != 1:
        raise ValueError("File {} is not accessible or is not in valid INI format".format(conf_file))

    if not config.has_section('conll-conv-settings'):
        raise ValueError("Error in config file {!r}: missing a section {!r}".format(conf_file, 'conll-conv-settings'))
    for option in ["gold_ud_layer", "json_corpus_dir"]:
        if not config.has_option('conll-conv-settings', option):
            raise ValueError("Error in config file {!r}: missing option {!r} a section {!r}".format(conf_file, option, 'conll-conv-settings'))

    if not config.has_section('eval-settings'):
        raise ValueError("Error in config file {!r}: missing a section {!r}".format(conf_file, 'eval-settings'))
    target_corpus_subsets = []
    corpus_restriction = ''
    for option in ["auto_ud_layer", "corpus_subsets", "remove_connegatives", "generate_num_cases", "show_mismatch_types", 
                   "create_match_log_file", "match_log_file_infix"]:
        if not config.has_option('eval-settings', option):
            raise ValueError("Error in config file {!r}: missing option {!r} a section {!r}".format(conf_file, option, 'eval-settings'))
        if option == "corpus_subsets":
            # Check corpus_subsets value
            subsets_str = (str(config['eval-settings']["corpus_subsets"])).strip()
            if subsets_str == 'None' or len(subsets_str) == 0:
                raise ValueError("Error in config file {!r}: bad value {!r} for option {!r}".format(conf_file, subsets_str, option))
            PERMITTED_SUBSETS = ['train', 'dev', 'test']
            subsets = subsets_str.split(',')
            for subset in subsets:
                if subset.lower() not in PERMITTED_SUBSETS:
                    raise ValueError( ("Error in config file {!r}: bad value {!r} for option {!r}. "+\
                                       "Permitted values are: {!r}").format(conf_file, subset, option, PERMITTED_SUBSETS) )
                else:
                    target_corpus_subsets.append( subset.lower() )

    if config.has_option('eval-settings', 'corpus_restriction'):
        corpus_restriction = (str(config['eval-settings']['corpus_restriction'])).strip().lower()
        if corpus_restriction not in ['edt', 'ewt', '']:
            raise ValueError( ("Error in config file {!r}: bad value {!r} for option {!r}. "+\
                               "Permitted values are: {!r}").format(conf_file, corpus_restriction, 
                               'corpus_restriction', ['edt', 'ewt', '']) )

    converter = UDMorphConverter(output_layer=config['eval-settings']["auto_ud_layer"],
                                 remove_connegatives=config['eval-settings']["remove_connegatives"], 
                                 generate_num_cases=config['eval-settings']["generate_num_cases"])

    in_dir = config['conll-conv-settings']["json_corpus_dir"]
    if not os.path.isdir(in_dir):
        raise FileNotFoundError('(!) Missing json files directory: {!r}. Use script 01_conv_ud_corpora_to_json.py to convert conll files to json.'.format(in_dir))
    for corpus_subset in target_corpus_subsets:
        comparator = UDMorphComparator(config['conll-conv-settings']["gold_ud_layer"], 
                                       config['eval-settings']["auto_ud_layer"], 
                                       show_mismatch_types=config['eval-settings']["show_mismatch_types"])
        req_file_infix = '-'+corpus_subset
        start = datetime.now()
        output_log_file = None
        if config['eval-settings']["create_match_log_file"]:
            infix = config['eval-settings']["match_log_file_infix"]
            if corpus_restriction not in infix:
                infix = infix + '_' + corpus_restriction
            start_time_str = start.strftime("%Y-%m-%d_%H%M%S")
            output_log_file = f'eval_{corpus_subset}_{infix}_log_{start_time_str}.txt'
            with open(output_log_file, 'w', encoding='utf-8') as out_f:
                pass
        if len(corpus_restriction) > 0:
            corpus_restriction = '_'+corpus_restriction
        c = 0
        for fname in tqdm(os.listdir(in_dir), ascii=True):
            if req_file_infix not in fname:
                continue
            if corpus_restriction not in fname:
                continue
            if fname.endswith('.json'):
                local_comparator = UDMorphComparator(config['conll-conv-settings']["gold_ud_layer"], 
                                                     config['eval-settings']["auto_ud_layer"])
                #print(fname)
                fpath = os.path.join(in_dir, fname)
                text = json_to_text(file=fpath)
                assert 'morph_analysis' in text.layers
                converter.tag(text)
                assert len(text[config['conll-conv-settings']["gold_ud_layer"]]) == len(text[config['eval-settings']["auto_ud_layer"]])
                match_strings = ['='*75]
                match_strings.append(fname)
                match_strings.append('='*75)
                for gold_ud, auto_ud in zip(text[config['conll-conv-settings']["gold_ud_layer"]], 
                                            text[config['eval-settings']["auto_ud_layer"]]):
                    match_strings.append('')
                    match_str = comparator.get_match_status_string(gold_ud.annotations, auto_ud.annotations)
                    local_comparator.get_match_status_string(gold_ud.annotations, auto_ud.annotations)
                    match_strings.append(match_str)
                doc_stats_str = local_comparator.get_total_stats_string()
                #print(total_stats_str)
                match_strings.append('')
                match_strings.append('-'*25)
                match_strings.append(doc_stats_str)
                match_strings.append('')
                if output_log_file is not None:
                    with open(output_log_file, 'a', encoding='utf-8') as out_f:
                        for s in match_strings:
                            out_f.write(s)
                            out_f.write('\n')
                c += 1
                #break

        if output_log_file is not None:
            with open(output_log_file, 'a', encoding='utf-8') as out_f:
                out_f.write('\n')
                out_f.write('='*75)
                out_f.write('\n')
                out_f.write('Results of evaluating on {} set'.format(corpus_subset))
                out_f.write('\n')
                out_f.write(comparator.get_total_stats_string(display_types=config['eval-settings']["show_mismatch_types"]))
                out_f.write('\n')

        print()
        print('Results of evaluating on {} set'.format(corpus_subset) )
        print('Texts read:  ', c)
        print(comparator.get_total_stats_string(display_types=config['eval-settings']["show_mismatch_types"]))
        print()
        print('Proc time:   {}'.format(datetime.now()-start))



