# EstMorphBERT - modified BERT model using morphological features (lemma + form) as tokens

The work was done for the Master's thesis titled "Adapting the BERT Model to Estonian Language"
Author: Raul Niit
Supervisors: Sven Laur and Hendrik Šuvalov

Code: https://github.com/raulniit/transformers
Thesis: https://comserv.cs.ut.ee/ati_thesis/datasheet.php?id=77365

# Scripts

Estonian National Corpus 2017 was used for developing and training the model

## 00_sõnastiku_loomine.py

Creating a vocabulary of 50 000 most common lemmas in the corpus.

## 01_korpuse_töötlemine.py

Converting the corpus files into .tsv format.

## 02_treeningandmete_loomine.py

Tokenization of the corpus files, which are kept in .json format.

## 03_eeltreenimine.py

Pre-training the BERT model (based on EstBERT).

## 04_kohandamine_MLM.py

Fine-tuning the pre-trained model for the masked language modelling task, computing accuracies of predicting the correct lemma and form.

## 04_kohandamine_NER.py

Fine-tuning the pre-trained model for the NER (Named entity recognition) task.

## 04_kohandamine_POS.py

Fine-tuning the pre-trained model for the POS (Part-of-speech tagging) task.

## 04_kohandamine_Rubric.py

Fine-tuning the pre-trained model for the Rubric classification task.