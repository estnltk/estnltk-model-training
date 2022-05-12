This module has a pipeline for using statistical methods with rule-based taggers to do NER tagging.
It also includes comparison to the existing NER tagger in EstNLTK.
The following diagram describes the workflow:

```mermaid
graph TD;
    parallel_ner_tagging.py-->entity_counting.py;
    entity_counting.py-->creating_rulesets.ipynb;
    creating_rulesets.ipynb-->curation.ipynb;
    curation.ipynb-->sentences_from_given_first_words_list.ipynb;
    curation.ipynb-->sentences_from_given_last_word_list.ipynb;
    curation.ipynb-->rule_based_ner_tagging.ipynb;
```

### parallel_ner_tagging.py
uses parallel computing on EstNLTK collections to apply the NER tagger on a large corpus.
Here it is applied on Koondkorpus but it can be used on other corpora as well.

```mermaid
graph LR;
    id1[collection without the NER layer]-->parallel_ner_tagging.py;
    parallel_ner_tagging.py-->id2[collection with NER layer];
```


### entity_counting.py
aggregates the data from the NER tagging file to CSV files
  divided by if the entity is composed of multiple tokens or one and for multiple
  tokens it separates the last tokens and all preceding tokens.

```mermaid
graph LR;
    id3[collection with NER layer]-->entity_counting.py;
    entity_counting.py-->id4[CSV file with counts of named entities - ner_counts.csv];
```

### curation.ipynb
shows how these count lists can be used to create rulesets for
  statistical tagging

```mermaid
graph LR;
    id5[CSV file with counts of named entities - ner_counts.csv]-->curation.ipynb;
    curation.ipynb-->id6[rulesets for statistical tagging - first_ruleset.pkl, last_ruleset.pkl];
```

### sentences_from_given_first_words_list.ipynb, sentences_from_given_last_word_list.ipynb

  create a list of EstNLTK text objects, each containing one sentence, where any
  of the given words in a list exists.

```mermaid
graph LR;
    id7[ruleset from a list of first words - first_ruleset.pkl ]-->sentences_from_given_first_words_list.ipynb;
    sentences_from_given_first_words_list.ipynb-->id8[all sentences containing these words - first_word_dataset.pkl];
    id9[ruleset from a list of last words - last_ruleset.pkl ]-->sentences_from_given_last_word_list.ipynb;
    sentences_from_given_last_word_list.ipynb-->id10[all sentences containing these words - last_word_dataset.pkl];
```
### rule_based_ner_tagging.py
uses the curated rulesets to perform statistical ner
  tagging

```mermaid
graph LR;
    id11[any ruleset from curation.ipynb]-->rule_based_ner_tagging.py;
    rule_based_ner_tagging.py-->id12[new NER layer];
```
### workflow_config.ini 
contains the configuration for all the files used here.
