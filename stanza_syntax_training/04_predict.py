#
#    Applies models to get predictions.
#    Supported models:
#       * stanza syntax (depparse)
#       * stanza syntax ensemble (depparse)
#    Implemented settings:
#       * full_data
#       * crossvalidation
#       * half_data
#       * smaller_data
#
import os, os.path
import re
import sys
from datetime import datetime

from conllu import parse_incr
from conllu.serializer import serialize_field

from stanza import Pipeline
from stanza.models.common.doc import Document
from stanza.utils.conll import CoNLL

import configparser

# ===============================================================
#  Run trained models / get predictions (MAIN)
# ===============================================================

def run_models_main( conf_file, subexp=None, dry_run=False ):
    '''
    Runs model(s) based on the configuration. 
    Settings/parameters of running model(s) will be read from the 
    given `conf_file`. 
    Executes sections in the configuration starting with prefix 
    'predict_stanza_'. 
    
    Optinally, if `subexp` is defined, then runs only that 
    sub-experiment and skips all other sub-experiments (in 
    crossvalidation, smaller_data and half_data experiments).
    '''
    # Parse configuration file
    config = configparser.ConfigParser()
    if conf_file is None or not os.path.exists(conf_file):
        raise FileNotFoundError("Config file {} does not exist".format(conf_file))
    if len(config.read(conf_file)) != 1:
        raise ValueError("File {} is not accessible or is not in valid INI format".format(conf_file))
    section_found = False
    for section in config.sections():
        # ------------------------------------------
        #  s t a n z a
        # ------------------------------------------
        if section.startswith('predict_stanza_'):
            section_found = True
            subexp_str = '' if subexp is None else f' ({subexp})'
            print(f'Running {section}{subexp_str} ...')
            experiment_type = config[section].get('experiment_type', 'full_data')
            experiment_type_clean = (experiment_type.strip()).lower()
            if experiment_type_clean not in ['full_data', 'crossvalidation', 'half_data', 'smaller_data']:
                raise ValueError('(!) Unexpected experiment_type value: {!r}'.format(experiment_type))
            if experiment_type_clean == 'full_data':
                # ------------------------------------------
                # 'full_data'
                # ------------------------------------------
                # train_file with path
                if not config.has_option(section, 'train_file'):
                    raise ValueError(f'Error in {conf_file}: section {section!r} is missing "train_file" parameter.')
                train_file = config[section]['train_file']
                if not os.path.isfile(train_file):
                    raise FileNotFoundError(f'Error in {conf_file}: invalid "train_file" value {train_file!r} in {section!r}.')
                # test_file with path
                if not config.has_option(section, 'test_file'):
                    raise ValueError(f'Error in {conf_file}: section {section!r} is missing "test_file" parameter.')
                test_file = config[section]['test_file']
                if not os.path.isfile(test_file):
                    raise FileNotFoundError(f'Error in {conf_file}: invalid "test_file" value {test_file!r} in {section!r}.')
                # output_dir
                if not config.has_option(section, 'output_dir'):
                    raise ValueError(f'Error in {conf_file}: section {section!r} is missing "output_dir" parameter.')
                output_dir = config[section]['output_dir']
                # use_estnltk=True and use_ensemble=False -- run model with estnltk's preprocessing and StanzaSyntaxTagger;
                # use_estnltk=True and use_ensemble=True  -- run model with estnltk's preprocessing and StanzaSyntaxEnsembleTagger;
                # use_estnltk=False                       -- run model on input feats loaded from conllu file;
                use_estnltk  = config[section].getboolean('use_estnltk', False)
                use_ensemble = config[section].getboolean('use_ensemble', False)
                if use_ensemble and not use_estnltk:
                    raise ValueError(f'Error in {conf_file}: section {section!r} conflicting '+\
                                     'configuration use_estnltk=False and use_ensemble=True. '+\
                                     'Cannot use ensemble tagger without estnltk.' )
                default_tagger_path = 'estnltk_neural.taggers.StanzaSyntaxTagger' if not use_ensemble else \
                                      'estnltk_neural.taggers.StanzaSyntaxEnsembleTagger'
                tagger_path = config[section].get('tagger_path', default_tagger_path)
                dry_run = config[section].getboolean('dry_run', dry_run)
                use_gpu = config[section].getboolean('use_gpu', False)
                output_prefix = config[section].get('output_file_prefix', 'predicted_')
                lang = config[section].get('lang', 'et')
                # Get model file or files
                model_file = None
                model_files = []
                if use_ensemble:
                    # predict with ensemble: get models_dir
                    if not config.has_option(section, 'models_dir'):
                        raise ValueError(f'Error in {conf_file}: section {section!r} is missing "models_dir" parameter.')
                    models_dir = config[section]['models_dir']
                    if not os.path.isdir(models_dir):
                        raise FileNotFoundError(f'Error in {conf_file}: invalid "models_dir" value {models_dir!r} in {section!r}.')
                    # collect all model files from the directory
                    model_file_name_pattern = re.compile( "^model_(.+)\.pt$")
                    for fname in os.listdir(models_dir):
                        if model_file_name_pattern.match(fname):
                            model_files.append( os.path.join(models_dir, fname) )
                    if len(model_files) == 0:
                        raise Exception( f'Error in {conf_file}: section {section!r}: Did not find any model files for '+\
                                          'the ensemble tagger from models_dir={models_dir!r}.' )
                else:
                    # predict with a single model: get model file with path
                    if not config.has_option(section, 'model_file'):
                        raise ValueError(f'Error in {conf_file}: section {section!r} is missing "model_file" parameter.')
                    model_file = config[section]['model_file']
                    if not os.path.isfile(model_file):
                        raise FileNotFoundError(f'Error in {conf_file}: invalid "model_file" value {model_file!r} in {section!r}.')
                # Run predictions
                if not dry_run:
                    start_time = datetime.now()
                    # Predict on train data
                    train_output = os.path.join(output_dir, f'{output_prefix}train.conllu')
                    if use_estnltk:
                        if not config.has_option(section, 'morph_layer'):
                            raise ValueError(f'Error in {conf_file}: section {section!r} is missing "morph_layer" parameter.')
                        morph_layer = config[section]['morph_layer']
                        if not use_ensemble:
                            # run StanzaSyntaxTagger
                            predict_with_stanza_tagger(train_file, morph_layer, model_file, train_output, 
                                                       tagger_path=tagger_path, lang=lang, use_gpu=use_gpu)
                        else:
                            # run StanzaSyntaxEnsembleTagger
                            predict_with_stanza_ensemble_tagger(train_file, morph_layer, model_files, train_output, 
                                                                tagger_path=tagger_path, lang=lang, use_gpu=use_gpu)
                    else:
                        # run vanilla stanza
                        predict_with_stanza(train_file, model_file, train_output, lang=lang, use_gpu=use_gpu)
                    # Predict on test data
                    test_output = os.path.join(output_dir, f'{output_prefix}test.conllu')
                    if use_estnltk:
                        if not config.has_option(section, 'morph_layer'):
                            raise ValueError(f'Error in {conf_file}: section {section!r} is missing "morph_layer" parameter.')
                        morph_layer = config[section]['morph_layer']
                        if not use_ensemble:
                            # run StanzaSyntaxTagger
                            predict_with_stanza_tagger(test_file, morph_layer, model_file, test_output, 
                                                       tagger_path=tagger_path, lang=lang, use_gpu=use_gpu)
                        else:
                            # run StanzaSyntaxEnsembleTagger
                            predict_with_stanza_ensemble_tagger(test_file, morph_layer, model_files, test_output, 
                                                                tagger_path=tagger_path, lang=lang, use_gpu=use_gpu)
                    else:
                        # run vanilla stanza
                        predict_with_stanza(test_file, model_file, test_output, lang=lang, use_gpu=use_gpu)
                    print(f'Total time elapsed: {datetime.now()-start_time}')
            elif experiment_type_clean in ['crossvalidation', 'half_data', 'smaller_data']:
                # ------------------------------------------
                # 'crossvalidation'
                # 'half_data'
                # 'smaller_data'
                # ------------------------------------------
                # input_dir (training conllu files)
                if not config.has_option(section, 'input_dir'):
                    raise ValueError(f'Error in {conf_file}: section {section!r} is missing "input_dir" parameter.')
                input_dir = config[section]['input_dir']
                if not os.path.isdir(input_dir):
                    raise FileNotFoundError(f'Error in {conf_file}: invalid "input_dir" value {input_dir!r} in {section!r}.')
                # models_dir
                if not config.has_option(section, 'models_dir'):
                    raise ValueError(f'Error in {conf_file}: section {section!r} is missing "models_dir" parameter.')
                models_dir = config[section]['models_dir']
                if not os.path.isdir(models_dir):
                    raise FileNotFoundError(f'Error in {conf_file}: invalid "models_dir" value {models_dir!r} in {section!r}.')
                # test_file with path
                if not config.has_option(section, 'test_file'):
                    raise ValueError(f'Error in {conf_file}: section {section!r} is missing "test_file" parameter.')
                test_file = config[section]['test_file']
                if not os.path.isfile(test_file):
                    raise FileNotFoundError(f'Error in {conf_file}: invalid "test_file" value {test_file!r} in {section!r}.')
                # output_dir
                if not config.has_option(section, 'output_dir'):
                    raise ValueError(f'Error in {conf_file}: section {section!r} is missing "output_dir" parameter.')
                output_dir = config[section]['output_dir']
                # use_estnltk=True  -- run model with estnltk's preprocessing and StanzaSyntaxTagger;
                # use_estnltk=False -- run model on input feats loaded from conllu file;
                use_estnltk = config[section].getboolean('use_estnltk', False)  # use estnltk's StanzaSyntaxTagger
                if config[section].getboolean('use_ensemble', False):
                    # Ensemble models are not supported here
                    raise NotImplementedError(f'Error in {conf_file}: section {section!r}: experiment_type='+\
                                              f'{experiment_type_clean!r} does not support ensemble models.')
                tagger_path = config[section].get('tagger_path', 'estnltk_neural.taggers.StanzaSyntaxTagger')
                dry_run = config[section].getboolean('dry_run', dry_run)
                use_gpu = config[section].getboolean('use_gpu', False)
                output_prefix = config[section].get('output_file_prefix', 'predicted_')
                lang = config[section].get('lang', 'et')
                morph_layer = None
                if use_estnltk:
                    if not config.has_option(section, 'morph_layer'):
                        raise ValueError(f'Error in {conf_file}: section {section!r} is missing "morph_layer" parameter.')
                    morph_layer = config[section]['morph_layer']
                # Patterns for capturing names of training sub-experiment files
                train_file_pat = r'(?P<exp>\d+)_train_all.conllu'
                # Override sub-experiment patterns (if required)
                if config.has_option(section, 'train_file_pat'):
                    train_file_pat = config[section]['train_file_pat']
                parser = 'stanza'
                # Run models
                bulk_predict( input_dir, models_dir, train_file_pat, test_file, 
                              output_dir, output_file_prefix=output_prefix, subexp=subexp, 
                              parser=parser, use_estnltk=use_estnltk, morph_layer=morph_layer, 
                              tagger_path=tagger_path, lang=lang, use_gpu=use_gpu, 
                              dry_run=dry_run )
    if not section_found:
        print(f'No section starting with "predict_stanza_" in {conf_file}.')


def bulk_predict( data_folder, models_folder, train_file_pattern, test_file_path, 
                  output_path, output_file_prefix='predicted_', subexp=None, 
                  parser='stanza', use_estnltk=False, morph_layer=None, 
                  tagger_path=None, lang='et', use_gpu=False, dry_run=False ):
    '''
    Runs models of multiple sub-experiments on (train/test) files from `data_folder`. 
    Outputs prediction conllu files to `output_path`. 
    
    Parameter `train_file_pattern` must be a string compilable into regexp pattern 
    that can be used to detect training data sets of all sub-experiments. 
    This patterns must have the named group 'exp', indicating part of the pattern 
    matching sub-experiment name. 
    
    Use parameter `subexp` to restrict predictions only to a single sub-experiment 
    instead of performing all sub-experiments. 
    This is useful when multiple instances of the Python are launched for 
    parallelization. 
    '''
    # Validate input arguments
    supported_parsers = ['stanza']
    if not isinstance(parser, str) or parser.lower() not in supported_parsers:
        raise ValueError( f'(!) Unexpected parser: {parser!r}. '+\
                          f'Supported parsers: {supported_parsers!r}' )
    parser = parser.lower()
    if parser == 'stanza' and tagger_path is None:
        tagger_path = 'estnltk_neural.taggers.StanzaSyntaxTagger'
    if not os.path.exists(data_folder) or not os.path.isdir(data_folder):
        raise Exception(f'(!) Missing or invalid data_folder {data_folder!r}')
    if not os.path.exists(models_folder) or not os.path.isdir(models_folder):
        raise Exception(f'(!) Missing or invalid models_folder {models_folder!r}')
    if not os.path.exists(test_file_path) or not os.path.isfile(test_file_path):
        raise Exception(f'(!) Missing or invalid test_file_path {test_file_path!r}')
    if use_estnltk and morph_layer is None:
        raise Exception(f'(!) Unexpected None value for morph_layer with use_estnltk')
    # Convert train_file_pattern to regular experssion
    train_file_regex = None
    if not isinstance(train_file_pattern, str):
        raise TypeError('train_file_pattern must be a string')
    try:
        train_file_regex = re.compile(train_file_pattern)
    except Exception as err:
        raise ValueError(f'Unable to convert {train_file_pattern!r} to regexp') from err
    if 'exp' not in train_file_regex.groupindex:
        raise ValueError(f'Regexp {train_file_pattern!r} is missing named group "exp"')
    # Collect experiment input files
    models_folder_files = [ fname for fname in os.listdir(models_folder) ]
    experiment_data = { 'train':[], 'test':[], 'models': [], 'numbers':[] }
    for fname in sorted( os.listdir(data_folder) ):
        m = train_file_regex.match(fname)
        if m:
            if not (fname.lower()).endswith('.conllu'):
                raise Exception( f'(!) invalid file {fname}: train file '+\
                                  'must have extension .conllu' )
            fpath = os.path.join(data_folder, fname)
            # Training file varies, depending on the sub set of data
            experiment_data['train'].append( fpath )
            # Test file is always the same
            experiment_data['test'].append( test_file_path )
            no = m.group('exp')
            if no not in experiment_data['numbers']:
                experiment_data['numbers'].append(no)
            # Find corresponding model from the models folder
            target_model_file = f"model_{no}.pt"
            model_found = False
            for model_fname in models_folder_files:
                if model_fname == target_model_file:
                    mfpath = os.path.join(models_folder, model_fname)
                    experiment_data['models'].append(mfpath)
                    model_found = True
                    break
            if not model_found:
                raise Exception(f'(!) Unable to find model {target_model_file} from {models_folder!r}')
    # Validate that we have all required files
    for subset in ['train']:
        if len(experiment_data[subset]) == 0:
            raise Exception(f'Unable to find any {subset} files '+\
                            f'matching {train_file_pattern!r} in dir {data_folder!r}.')
        if len(experiment_data[subset]) != len(experiment_data['numbers']):
            no1 = len(experiment_data[subset])
            no2 = len(experiment_data['numbers'])
            raise Exception(f'Number of {subset} files ({no1}) does not match '+\
                            f'the number of experiments ({no2}).')
        if len(experiment_data[subset]) != len(experiment_data['models']):
            no1 = len(experiment_data[subset])
            no2 = len(experiment_data['models'])
            raise Exception(f'Number of {subset} files ({no1}) does not match '+\
                            f'the number of models ({no2}).')
    if subexp is not None:
        if subexp not in experiment_data['numbers']:
            raise ValueError( f'(!) sub-experiment {subexp!r} not in collected '+\
                              f'experiment names: {experiment_data["numbers"]}.' )
    # Launch experiments
    if not dry_run:
        start_time = datetime.now()
        for i in range( len(experiment_data['numbers']) ):
            exp_no     = experiment_data['numbers'][i]
            train_file = experiment_data['train'][i]
            test_file  = experiment_data['test'][i]
            model_file = experiment_data['models'][i]
            if subexp is not None and exp_no != subexp:
                # Skip other experiments
                continue
            if parser == 'stanza':
                # Predict on train data
                train_output = os.path.join(output_path, f'{output_file_prefix}train_{exp_no}.conllu')
                if use_estnltk:
                    predict_with_stanza_tagger(train_file, morph_layer, model_file, train_output, 
                                               tagger_path=tagger_path, lang=lang, use_gpu=use_gpu)
                else:
                    predict_with_stanza(train_file, model_file, train_output, lang=lang, use_gpu=use_gpu)
                # Predict on test data
                test_output = os.path.join(output_path, f'{output_file_prefix}test_{exp_no}.conllu')
                if use_estnltk:
                    predict_with_stanza_tagger(test_file, morph_layer, model_file, test_output, 
                                               tagger_path=tagger_path, lang=lang, use_gpu=use_gpu)
                else:
                    predict_with_stanza(test_file, model_file, test_output, lang=lang, use_gpu=use_gpu)
            print()
        print()
        print(f'Total time elapsed: {datetime.now()-start_time}')

# ========================================================================
#  Stanza interface: run models to get depparse predictions
#  Two ways:
#  1) run model with estnltk's preprocessing and StanzaSyntax(Ensemble)Tagger;
#  2) run model on input feats loaded from conllu file;
# ========================================================================

def create_estnltk_document( input_path, morph_layer='morph_extended', 
                                         syntax_layer='gold_syntax' ):
    """
    Loads given CONLLU file as estnltk's Text object. 
    Preannotates text: adds tokenization layers and morph_layer 
    (either 'morph_extended' or 'morph_analysis').
    Returns loaded Text object.
    
    Requires: estnltk v1.7.2+
    
    :param input_path: path to conllu file to be loaded
    :param morph_layer: name of estnltk's morphological analysis layer
    :param syntax_layer: name of syntactic analysis layer loaded from file
    :return: estnltk Text object
    """
    from estnltk import Text
    from estnltk.taggers import WhiteSpaceTokensTagger
    from estnltk.taggers import PretokenizedTextCompoundTokensTagger
    from estnltk.converters.conll.conll_importer import conll_to_text
    text_obj = conll_to_text(input_path, syntax_layer)
    assert 'words' in text_obj.layers
    assert 'sentences' in text_obj.layers
    if 'compound_tokens' not in text_obj.layers:
        (WhiteSpaceTokensTagger()).tag( text_obj )
        (PretokenizedTextCompoundTokensTagger()).tag( text_obj )
    text_obj.tag_layer( morph_layer )
    return text_obj

def create_stanza_document(input_path):
    """
    Loads sentences from given CONLLU file and creates stanza's Document. 
    Document will be pretagged: it contains id, text, lemma, upos, xpos, 
    feats values loaded from the CONLLU file, and empty values in place 
    of other conllu fields. 
    Returns loaded Document.
    
    :param input_path: path to conllu file to be loaded
    :return: stanza Document
    """
    with open(input_path, 'r', encoding='utf-8') as conllu_file:
        data = []
        for tokenlist in parse_incr(conllu_file):
            pretagged_sent = []
            for word in tokenlist:
                if not isinstance(word['id'], int):
                    # Because stanza cannot handle ellipsis (considers it 
                    # a multi-word), we leave ellipsis word out
                    continue
                word_feats = {
                    'id': word['id'],
                    'text': word['form'],
                    'lemma': word['lemma'],
                    'upos': word['upos'],
                    'xpos': word['xpos'],
                    'feats': serialize_field( word['feats'] )
                }
                pretagged_sent.append(word_feats)
            data.append(pretagged_sent)
    # create Document-obj from sentences of the pretagged file
    return Document(data)

def predict_with_stanza(input_path, model_path, output_path, lang='et', use_gpu=False):
    '''
    Applies stanza's model on given input CONLLU file to get depparse predictions. 
    Saves predictions to output CONLLU file.
    
    :param input_path:  path to conllu file to be annotated
    :param model_path:  path to depparse model to be used for making predictions
    :param output_path: path to output conllu file
    '''
    config = {
        'processors': 'depparse',  # Comma-separated list of processors to use
        'lang': lang,  # Language code for the language to build the Pipeline in
        'depparse_pretagged': True,
        'depparse_model_path': model_path,
        'download_method': 0, # NONE will not download anything
        'use_gpu': use_gpu
    }
    nlp = Pipeline(**config)
    doc = create_stanza_document(input_path)
    nlp(doc)
    output_dir, output_fname = os.path.split(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    write_stanza_doc_to_conll(doc, output_path)

def predict_with_stanza_tagger(input_path, morph_layer, model_path, output_path, 
                               tagger_path='estnltk_neural.taggers.StanzaSyntaxTagger', 
                               lang='et', use_gpu=False):
    '''
    Applies estnltk's StanzaSyntaxTagger on given input CONLLU file to get depparse predictions. 
    Uses estnltk's preprocessing to load and re-annotate document (adds morph_layer). 
    Saves predictions to output CONLLU file.

    By default, imports tagger from 'estnltk_neural.taggers.StanzaSyntaxTagger', but you can 
    use `tagger_path` to overwrite the importing path. Use this if you've customized the tagger 
    (e.g. made fixes for it), and want to test it out (instead of the default version). 

    Requires: estnltk v1.7.2+

    :param input_path:  path to conllu file to be annotated
    :param morph_layer: name of estnltk's morphological analysis layer
    :param model_path:  path to depparse model to be used for making predictions
    :param output_path: path to output conllu file
    :param tagger_path: full import path of StanzaSyntaxTagger
    '''
    tagger_loader = \
        create_stanza_tagger_loader( tagger_path, model_path, morph_layer )
    tagger = tagger_loader.tagger  # Load tagger
    text_obj = create_estnltk_document(input_path, morph_layer=morph_layer)
    tagger.tag(text_obj)
    output_dir, output_fname = os.path.split(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    write_estnltk_text_to_conll(text_obj, tagger.output_layer, output_path)

def predict_with_stanza_ensemble_tagger(input_path, morph_layer, model_paths, output_path, 
                                        tagger_path='estnltk_neural.taggers.StanzaSyntaxEnsembleTagger', 
                                        lang='et', use_gpu=False, verbose=True ):
    '''
    Applies estnltk's StanzaSyntaxEnsembleTagger on given input CONLLU file to get depparse predictions. 
    Uses estnltk's preprocessing to load and re-annotate document (adds morph_layer). 
    Saves predictions to output CONLLU file.
    
    By default, imports tagger from 'estnltk_neural.taggers.StanzaSyntaxEnsembleTagger', but you can 
    use `tagger_path` to overwrite the importing path. Use this if you've customized the tagger 
    (e.g. made fixes for it), and want to test it out (instead of the default version). 

    Requires: estnltk v1.7.2+

    :param input_path:  path to conllu file to be annotated
    :param morph_layer: name of estnltk's morphological analysis layer
    :param model_path:  path to depparse model to be used for making predictions
    :param output_path: path to output conllu file
    :param tagger_path: full import path of StanzaSyntaxEnsembleTagger
    '''
    tagger_loader = \
        create_stanza_ensemble_tagger_loader( tagger_path, model_paths, morph_layer )
    tagger = tagger_loader.tagger  # Load tagger
    if verbose:
        print(f'Loaded {tagger_path!r} with {len(model_paths)} models for prediction.')
    text_obj = create_estnltk_document(input_path, morph_layer=morph_layer)
    if verbose:
        print(f'Preprocessed {input_path!r}.')
    tagger.tag(text_obj)
    if verbose:
        print(f'Parsed {input_path!r} with the ensemble tagger.')
    output_dir, output_fname = os.path.split(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    write_estnltk_text_to_conll(text_obj, tagger.output_layer, output_path)

def create_stanza_tagger_loader( tagger_path, model_path, input_morph_layer, use_gpu=False ):
    '''Creates estnltk's TaggerLoader for customized importing of StanzaSyntaxTagger.'''
    from estnltk_core.taggers import TaggerLoader
    return TaggerLoader( 'stanza_syntax', 
                         ['sentences', input_morph_layer, 'words'], 
                         tagger_path, 
                         output_attributes=('id', 'lemma', 'upostag', 'xpostag', 'feats', 'head', 'deprel', 'deps', 'misc'),
                         parameters={ 'input_morph_layer': input_morph_layer, 
                                      'input_type': input_morph_layer, 
                                      'depparse_path': model_path, 
                                      'use_gpu': use_gpu } )
                                      
def create_stanza_ensemble_tagger_loader( tagger_path, model_paths, input_morph_layer, use_gpu=False ):
    '''Creates estnltk's TaggerLoader for customized importing of StanzaSyntaxEnsembleTagger.'''
    from estnltk_core.taggers import TaggerLoader
    return TaggerLoader( 'stanza_ensemble_syntax', 
                         ['sentences', input_morph_layer, 'words'], 
                         tagger_path, 
                         output_attributes=('id', 'lemma', 'upostag', 'xpostag', 'feats', 'head', 'deprel', 'deps', 'misc'),
                         parameters={ 'input_morph_layer': input_morph_layer, 
                                      'model_paths': model_paths, 
                                      'use_gpu': use_gpu } )

def write_stanza_doc_to_conll(doc, output_path):
    '''Writes given stanza Document to CoNLLU format output file.'''
    conll = CoNLL.convert_dict(doc.to_dict())
    with open(output_path, 'w', encoding='utf-8') as fout:
        for sentence in conll:
            for word in sentence:
                fout.write('\t'.join(word) + '\n')
            fout.write('\n')
        fout.write('\n' * 2)

def write_estnltk_text_to_conll(text, syntax_layer, output_path):
    '''Writes given estnltk's Text with syntax_layer to CoNLLU format output file.'''
    from estnltk.converters.conll.conll_exporter import layer_to_conll
    text_conll_str = \
        layer_to_conll(text, syntax_layer, preserve_ambiguity=False)
    with open(output_path, 'w', encoding='utf-8') as fout:
        fout.write( text_conll_str )
        fout.write('\n')

# ========================================================================

if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception('(!) Missing input argument: name of the configuration INI file.')
    conf_file = sys.argv[1]
    subexp = None
    if len(sys.argv) > 2:
        subexp = sys.argv[2]
    run_models_main( conf_file, subexp=subexp )