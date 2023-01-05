#
#  * Converts Estonian UD corpora from CONLL format to Estnltk Text objects;
#     * Loads conversion configuration from an INI file (section [conll-conv-settings]);
#     * Adds morphological analyses via VabamorfAnalyzer or VabamorfTagger (depending on config);
#     * Saves Text objects as json files into output json directory;
#

import os, os.path, sys
import configparser

from tqdm import tqdm
from datetime import datetime

from estnltk import Text
from estnltk.taggers import VabamorfAnalyzer, VabamorfTagger
from estnltk.converters import text_to_json
from estnltk.converters.conll.conll_importer import conll_to_texts_list

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
    analyser_str = ''
    vm_analyser = None
    for option in ["edt_corpus_dir", "ewt_corpus_dir", "gold_ud_layer", "morph_analyser", "json_corpus_dir"]:
        if not config.has_option('conll-conv-settings', option):
            raise ValueError("Error in config file {!r}: missing option {!r} a section {!r}".format(conf_file, option, 'conll-conv-settings'))
        if option == "morph_analyser":  # type of morphological analysis used
            analyser_str = (str(config['conll-conv-settings']["morph_analyser"])).strip()
            ANALYSERS = ['VabamorfAnalyzer', 'VabamorfTagger']
            if len(analyser_str) > 0 and analyser_str != 'None' and analyser_str not in ANALYSERS:
                raise ValueError("Error in config file {!r}: bad value {!r} for option {!r}. Expected values: {!r}".format(conf_file, analyser_str, option, ANALYSERS))
        if option == "json_corpus_dir":  # output directory
            json_dir = str(config['conll-conv-settings']["json_corpus_dir"])
            if len(json_dir) == 0 or json_dir == 'None':
                raise ValueError("Error in config file {!r}: bad value {!r} for option {!r}.".format(conf_file, json_dir, option))
        if option == "gold_ud_layer":    # name of the gold standard UD layer
            gold_layer = str(config['conll-conv-settings']["gold_ud_layer"])
            if len(gold_layer) == 0 or gold_layer == 'None':
                raise ValueError("Error in config file {!r}: bad value {!r} for option {!r}.".format(conf_file, gold_layer, option))

    # Check existence of input directories
    input_dirs = [ config['conll-conv-settings']["edt_corpus_dir"], config['conll-conv-settings']["ewt_corpus_dir"] ]
    for in_dir in input_dirs:
        if not os.path.isdir(in_dir):
            raise FileNotFoundError('(!) Missing or unexpected input directory {!r}'.format(in_dir))
    # Create output directory
    out_dir = config['conll-conv-settings']["json_corpus_dir"]
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    # Create analysers
    if len(analyser_str) > 0:
        if analyser_str == 'VabamorfAnalyzer':
            vm_analyser = VabamorfAnalyzer()
        elif analyser_str == 'VabamorfTagger':
            vm_analyser = VabamorfTagger()
    filter_infixes = []
    gold_ud_morph = config['conll-conv-settings']["gold_ud_layer"]
    c = 0
    start = datetime.now()
    for in_dir in input_dirs:
        for fname in tqdm(os.listdir(in_dir), ascii=True):
            if filter_infixes and not [inf for inf in filter_infixes if inf in fname]:
                continue
            if fname.endswith('.conllu'):
                fpath = os.path.join(in_dir, fname)
                texts = conll_to_texts_list(fpath, syntax_layer=gold_ud_morph, remove_empty_nodes=False)
                for tid, text in enumerate(texts):
                    text.meta['origin_file'] = fname
                    text.meta['origin_corpus'] = 'edt' if '_edt-' in fname else 'ewt'
                    if vm_analyser is not None:
                        text.tag_layer('compound_tokens')
                        vm_analyser.tag(text)
                    #
                    # Fix id and head annotations, remove parent span and children
                    #
                    syntax_layer = text[gold_ud_morph]
                    syntax_layer.attributes = ("id", "lemma", "upostag", "xpostag", "feats", "head", "deprel", "deps", "misc")
                    syntax_layer.secondary_attributes = []
                    syntax_layer.serialisation_module = None
                    for syntax_span in syntax_layer:
                        new_annotation = { k : syntax_span.annotations[0][k] for k in syntax_layer.attributes }
                        if not isinstance(new_annotation['id'], int) and new_annotation['id'] is not None:
                            new_id = ''.join([str(s) for s in new_annotation['id']])
                            print('id fix:', new_annotation['id'], ' --> ', new_id)
                            new_annotation['id'] = new_id
                        if not isinstance(new_annotation['head'], int) and new_annotation['head'] is not None:
                            new_head = ''.join([str(s) for s in new_annotation['head']])
                            print('head fix:', new_annotation['head'], ' --> ', new_head)
                            new_annotation['head'] = new_head
                        syntax_span.clear_annotations()
                        syntax_span.add_annotation( new_annotation )
                    #
                    # Write output
                    #
                    out_fname = fname.replace('.conllu', '')
                    out_fname += f'_{tid:03d}'
                    out_fname += '.json'
                    out_fpath = os.path.join(out_dir, out_fname)
                    text_to_json(text, out_fpath)
                c += len(texts)
                #break
            #break

    print()
    print('Texts converted:  ', c)
    print('Proc time:        {}'.format(datetime.now()-start))


