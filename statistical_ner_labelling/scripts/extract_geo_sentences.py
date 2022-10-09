from estnltk.vabamorf.morf import synthesize
from estnltk.taggers.system.rule_taggers import AmbiguousRuleset, StaticExtractionRule, SubstringTagger
from estnltk.storage.postgres import PostgresStorage
from estnltk_core.layer_operations import split_by_sentences
from tqdm import tqdm
import pickle


import sys
import configparser
import os




#create a ruleset from list
def list_to_ruleset(rule_list):
    ruleset = AmbiguousRuleset()
    for word in rule_list:
        rule = StaticExtractionRule(pattern=word)
        ruleset.add_rules([rule])
    return ruleset


def decorate_fun(text, span, annotation):
    #do not tag if followed by alphabetic letter
    if len(text.text) > span.end and text.text[span.end].isalpha():
        return None
    #do not tag if it not preceded by whitespace
    if span.start != 0 and text.text[span.start-1] != ' ':
        return None
    return annotation



if __name__ == '__main__':

    config = configparser.ConfigParser()
    # first command line argument is the config file
    conf_file = sys.argv[1]
    file_name = os.path.abspath(os.path.expanduser(os.path.expandvars(str(conf_file))))

    config.read(file_name)

    cases = [
        ('n', 'nimetav'),
        ('g', 'omastav'),
        ('p', 'osastav'),
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

    with open(os.path.abspath(os.path.expanduser(os.path.expandvars(config['extract-configuration']['word_list_file']))),'r',encoding='UTF-8') as f:
        words = f.readlines()
    all_forms = []
    for word in words:
        for case, name in cases:
            for form in synthesize(word, 'sg ' + case, 'S'):
                all_forms.append(form)
            for form in synthesize(word, 'pl ' + case, 'S'):
                all_forms.append(form)

    output_layer = config['extract-configuration']['output_layer']
    ner_layer = config['extract-configuration']['ner_layer']
    sentences_layer = config['database-configuration']['sentences_layer']
    words_layer = config['database-configuration']['words_layer']
    subtagger = SubstringTagger(ruleset=list_to_ruleset(all_forms), ignore_case=False, global_decorator=decorate_fun,
                                output_layer=output_layer)

    storage = PostgresStorage(host=config['database-configuration']['server'],
                              port=config['database-configuration']['port'],
                              dbname=config['database-configuration']['database'],
                              user=config['database-configuration']['username'],
                              password=config['database-configuration']['password'],
                              schema=config['database-configuration']['schema'],
                              role=config['database-configuration']['role'])

    collection_name = config['database-configuration']['collection']
    collection = storage[collection_name]

    collection.create_layer(tagger=subtagger)

    '''
    collection.selected_layers = []
    collection.selected_layers.append(words_layer)
    collection.selected_layers.append(sentences_layer)
    collection.selected_layers.append(ner_layer)
    collection.selected_layers.append(output_layer)

    filtered_sents = []

    print("Filtering sentences")
    for text in tqdm(collection):
        sents = split_by_sentences(text, layers_to_keep=[output_layer, ner_layer, words_layer])
        for sent in sents:
            if len(sent[output_layer]) > 0:
                filtered_sents.append(sent)
                
    sampled_sentences = []
    for text_id, text_obj in collection.select(layers=[output_layer, sentences_layer]).sample_from_layer(output_layer, 5, seed=0.5):
        for term in text_obj[output_layer]:
            for sentence in text_obj[sentences_layer]:
                if sentence.end > term.start:
                    sampled_sentences.append(sentence)
                    break

    with open('geo_last_words_dataset_koond.pkl', 'wb') as f:
        pickle.dump(filtered_sents, f)
    with open("geo_sampled_sentences.pkl",'wb') as f:
        pickle.dump(sampled_sentences, f)
    '''
