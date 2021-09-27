import csv
import numpy as np

from datasets import Dataset
from psycopg2.sql import SQL, Literal
from estnltk import Text
from datasets import load_dataset
from transformers import logging
logging.set_verbosity_warning()

from pipelines.step03_BERT_fine_tuning import SequenceClassification
from pipelines.step03_BERT_fine_tuning.dataloaders import Sequences

import configparser

from os import listdir
from os.path import isfile, join


config = configparser.ConfigParser()
config.read('run_config.ini')

pretrained_model_path = config['path']['output_model_folder']
seq_training_data_folder = config['path']['seq_training_data_folder']
seq_training_model_path = config['path']['seq_training_model_folder']

training_files = [seq_training_data_folder + "/" + f for f in listdir(seq_training_data_folder)
                  if isfile(join(seq_training_data_folder, f))]

print(training_files)

dl = Sequences.Tsv(
            training_files,
            X="text",
            y="y"
        )

map_args = {
    # make sure that tokenization creates vectors of equal length when you use
    #"batched": True (also set "max_length": <a number>, "padding": "max_length" in tokenization args)
    "batched": True,
}

tokenizer_args = {
    "lowercase": False,
    # also providing the additinal special tokens, because the tokenizer does not know about these yet
}
tokenization_args = {
    "max_length": 128,
    "padding": "max_length",
    "truncation": True
}
training_args = {
    "output_dir": seq_training_model_path,
    "num_train_epochs": 1,
    "per_device_train_batch_size": 8,
    "per_device_eval_batch_size": 8
}

# fine-tuning the model
SequenceClassification.finetune_BERT(pretrained_model_path, dl, True, map_args, tokenizer_args, tokenization_args, training_args)
SequenceClassification.evaluate(seq_training_model_path, dl, map_args, tokenizer_args, tokenization_args, training_args)
