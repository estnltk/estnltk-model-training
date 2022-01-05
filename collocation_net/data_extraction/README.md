# Data Preprocessing

This folder contains files used for preprocessing the collocation net sqlite database files. Each file must contain the columns lemma1, pos1, lemma2, pos2 and count. Lemma1 and lemma2 are the lemmas of the two words that form the collocation, pos1 and pos2 are their POS-tags respectively. Count shows how many times the collocation appeared in the Estonian Koondkorpus.

Each collocation type has common main preprocessing steps. First collocations that are above the threshold need to be fetched. From those we need to remove collocations where the words are not connected to any other words. Finally we need to transform the data into a suitable format for the LDA model. For these common steps there are functions in the `data_preprocessing.py` file.

In addition certain collocation types may include some additional preprocessing steps. Currently these additional steps are removing collocations where one of the words' POS-tags doesn't fit the word type it should be and removing words that are infrequent if word count is too large. These steps are done in each Jupyter Notebook file, if necessary, as they need to be checked separately. For possible new collocations these steps need to be thought of as well.

The file `extract_noun_adjective_pairs.ipynb` in the `noun_adjective` folder showcases how initial noun-adjective collocations were extracted.