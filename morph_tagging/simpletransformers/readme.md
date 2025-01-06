## A simpletransformers adaption

This is a modified version of the simpletransformers library that was used in [BertMorphTagger](../).

File [`ner_model.py`](./ner/ner_model.py) has been modified to allow training the model only on the word "muna" with functions `_calculate_loss_muna` [2044-2083] and `_calculate_loss_muna_all` [2085-2118].

The basis of modifications is simpletransformers version 0.70.1, tagged repository [https://github.com/ThilinaRajapakse/simpletransformers/tree/v0.70.1](https://github.com/ThilinaRajapakse/simpletransformers/tree/v0.70.1) (2024-05-29).