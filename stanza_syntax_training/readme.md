## Training and evaluating StanzaSyntax(Ensemble)Tagger's models

This repository contains code for training and evaluating StanzaSyntax(Ensemble)Tagger's models on [Estonian UD corpus](https://github.com/UniversalDependencies/UD_Estonian-EDT).

### Pre-requisites

* Install [estnltk](https://github.com/estnltk/estnltk) (version 1.7.2+ is required);
* Install [stanza](https://github.com/stanfordnlp/stanza) (we used version 1.4.2); 
* Download and unpack [Estonian UD corpus](https://github.com/UniversalDependencies/UD_Estonian-EDT/tags) (we used version 2.6);

### Configuration files

Most important settings of data pre-processing, training and evaluation are defined in configuration INI files.
Configuration can be used for changing input and output paths of each processing step, and also for changing parameters of data preparation and training. Current configurations in brief: 

*  `conf_edt_v26_stanza_morph_analysis_full.ini` -- preannotates UD corpus (v2.6) with lemmas/postags/feats from estnltk's `morph_analysis` layer, trains `StanzaSyntaxTagger`'s model on this preannotated data, and tests the model on train+dev/test sets and calculates LAS/UAS scores;

*  `conf_edt_v26_stanza_morph_extended_full.ini` -- preannotates UD corpus (v2.6) with lemmas/postags/feats from estnltk's `morph_extended` layer, trains `StanzaSyntaxTagger`'s model on this preannotated data, and tests the model on train+dev/test sets and calculates LAS/UAS scores;

*  `conf_edt_v26_stanza_ensemble_morph_extended.ini` -- preannotates UD corpus (v2.6) with lemmas/postags/feats from estnltk's `morph_extended` layer, splits the training and dev sets into 10 subsets, trains 10 `StanzaSyntaxEnsembleTagger`'s models on these subsets, and tests these models with `StanzaSyntaxEnsembleTagger` and calculates corresponding LAS/UAS scores;

Crucial parts of configurations are paths: once you have downloaded and unpacked an UD corpus, please make sure to define  correct paths in configration files. Assumingly, all paths should be relative and point to sub directories of the code execution directory.

### Processing steps (scripts)

* `01_ud_preprocessing.py` -- Converts gold standard UD corpus to EstNLTK's format: overwrites values of `lemma`, `upos`, `xpos` and `feats` with EstNLTK's automatic morphological analyses (from layers `morph_analysis` / `morph_extended`). Executes all sections starting with `preannotation_` in input configuration file. Example usage:

	* `python  01_ud_preprocessing.py  conf_edt_v26_stanza_morph_extended_full.ini`

* `02_split_data.py` -- Creates data splits (or joins) for model training and evaluation. Executes all sections starting with `split_` and `join_` in input configuration file. Example usage:

	* `python  02_split_data.py  conf_edt_v26_stanza_morph_extended_full.ini`

* `03_train.py` -- Trains stanza models. Executes all sections starting with `train_stanza_` in input configuration file. Example:

	* `python  03_train.py  conf_edt_v26_stanza_morph_extended_full.ini`

* `04_predict.py` -- Applies trained stanza models on evaluation data to get predictions. Executes all sections starting with `predict_stanza_` in input configuration file. Example:

	* `python  04_predict.py  conf_edt_v26_stanza_morph_extended_full.ini`

* `05_evaluate.py` -- Looks through all configuration files in the execution directory, and performs evaluations defined in configuration files: compares predicted files to gold standard files and calculates LAS/UAS scores. Executes all sections starting with `eval_` in configuration files. Saves results into file `results.csv` in a sub directory closest to the execution directory (for given configurations, the path will be: `edt_2.6/results.csv`). Example usage:

	* `python  05_evaluate.py`

Note: configurations also contain overlapping parts, e.g. once you've run UD preprocessing with `conf_edt_v26_stanza_morph_extended_full.ini`, you do not need to run UD preprocessing again with `conf_edt_v26_stanza_ensemble_morph_extended.ini`;