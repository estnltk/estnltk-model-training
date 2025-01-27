# Training Stanza parser on shortened sentences

## Prerequisites

* Packages: [EstNLTK_neural](https://github.com/estnltk/estnltk/tree/main/estnltk_neural) version 1.7.3+, [stanza](https://stanfordnlp.github.io/stanza/) 1.8.2+;

* `cut_sentences_train.conllu` and `cut_sentences_dev.conllu` -- training and dev files with shortened sentences, based on [EDT 2.11](https://github.com/UniversalDependencies/UD_Estonian-EDT/releases/tag/r2.11). Values of CONLL-U fields `upos`, `xpos` and `feats` must be automatically overwritten with values from EstNLTK's `morph_extended` layer. The script for preparing these files can be found at:  https://github.com/estnltk/estnltk-model-data/tree/main/ud_syntax/data_augmentation/phrase_omissions (`aggregate_datasets.py`)

* `et_edt-ud-train-morph_extended.conllu`, `et_edt-ud-dev-morph_extended.conllu`, `et_edt-ud-test-morph_extended.conllu` -- training, dev and test sets of [EDT 2.11](https://github.com/UniversalDependencies/UD_Estonian-EDT/releases/tag/r2.11). Values of CONLL-U fields `upos`, `xpos` and `feats` must be automatically overwritten with values from EstNLTK's `morph_extended` layer. Use scripts from https://github.com/estnltk/syntax_experiments/tree/ablation_experiments for preparing the files: download & unpack [EDT 2.11](https://github.com/UniversalDependencies/UD_Estonian-EDT/releases/tag/r2.11), and run `python  01_ud_preprocessing.py  conf_edt_v211_Stanza_ME_full.ini` to perform required preprocessing. 

* (Optional) `model_morph_extended.pt` -- Stanza's parser model trained on [EDT 2.11](https://github.com/UniversalDependencies/UD_Estonian-EDT/releases/tag/r2.11) with `morph_extended` annotations. Use scripts from https://github.com/estnltk/syntax_experiments/tree/ablation_experiments for creating the model: download & unpack [EDT 2.11](https://github.com/UniversalDependencies/UD_Estonian-EDT/releases/tag/r2.11), and run 1) `python  01_ud_preprocessing.py  conf_edt_v211_Stanza_ME_full.ini`, 2) `python  02_split_data.py  conf_edt_v211_Stanza_ME_full.ini` and  3)`python  03_train_stanza.py  conf_edt_v211_Stanza_ME_full.ini` to create the model. 

* (Optional) `original_sentences_train.conllu` and `original_sentences_dev.conllu` -- training and dev files containing original (uncut) variants of sentences from `cut_sentences_train.conllu` and `cut_sentences_dev.conllu`. The script for preparing these files can be found at:  https://github.com/estnltk/estnltk-model-data/tree/main/ud_syntax/data_augmentation/phrase_omissions (`aggregate_datasets.py`)


## Preprocessing

* In order to get input files for training, concatenate full length training and dev files with corresponding shortened files:

    `$ python  02x_concat_conllu_files.py  et_edt-ud-train-morph_extended.conllu  cut_sentences_train.conllu  et_edt-ud-train-with-cut-train-morph_extended.conllu`

    `$ python  02x_concat_conllu_files.py  et_edt-ud-dev-morph_extended.conllu  cut_sentences_dev.conllu  et_edt-ud-dev-with-cut-train-morph_extended.conllu`

## Training and evaluation 

* For training the phrase omissions model, run: 
`python  03_train_stanza.py  conf_edt_v211_Stanza_ME_phrase_omissions.ini`
* After training is completed, run `python  04_predict_stanza.py  conf_edt_v211_Stanza_ME_phrase_omissions.ini` to get model's predictions on `cut_sentences_*.conllu` and `et_edt-ud-test-morph_extended.conllu` files.
* For evaluating the model on `et_edt-ud-test-morph_extended.conllu`, run `python  05_evaluate.py  conf_edt_v211_Stanza_ME_phrase_omissions.ini`.

## (optional) Comparison with the initial model

* If you also have the initial model (`model_morph_extended.pt` -- the model trained on full EDT 2.11 data, without any phrase omissions), run `python  04_predict_stanza.py  conf_edt_v211_Stanza_ME_original.ini` to get model's predictions on `cut_sentences_*.conllu` and `et_edt-ud-test-morph_extended.conllu` files.
* For evaluating the initial model on `et_edt-ud-test-morph_extended.conllu`, run `python  05_evaluate.py  conf_edt_v211_Stanza_ME_original.ini`.

## Results

* Evaluation results (LAS & UAS on test set): [`results_et_edt-ud-test-morph_extended.conllu.csv`](results_et_edt-ud-test-morph_extended.conllu.csv)
* Trained model: [stanza_syntax_phrase_omissions_2024-12-13.zip](https://s3.hpc.ut.ee/estnltk/models/stanza_models_experimental/stanza_syntax_phrase_omissions_2024-12-13.zip)

