from pipelines.step03_BERT_fine_tuning import TokenClassification
from pipelines.step03_BERT_fine_tuning.dataloaders import Tokens

import configparser

from os import listdir
from os.path import isfile, join
import os

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + '/' + 'run_config.ini')

pretrained_model_path = os.path.dirname(os.path.abspath(__file__)) + '/' + config['path']['output_model_folder']
token_training_data_folder = os.path.dirname(os.path.abspath(__file__)) + '/' + config['path']['token_training_data_folder']
token_training_model_path = os.path.dirname(os.path.abspath(__file__)) + '/' + config['path']['token_training_model_folder']

training_files = [token_training_data_folder + "/" + f for f in listdir(token_training_data_folder)
                  if isfile(join(token_training_data_folder, f))]

dl = Tokens.Tsv(training_files)

map_args = {
    "batched": True,
}
tokenizer_args = {
    "lowercase": False,
}
tokenization_args = {
    "max_length": 128,
    "padding": "max_length",
    "truncation": True
}
training_args = {
    "output_dir": token_training_model_path,
    "overwrite_output_dir": True,
    "num_train_epochs": 1,
    "per_device_train_batch_size": 8,
    "per_device_eval_batch_size": 8
}

# First fine-tuning the model
TokenClassification.finetune_BERT(pretrained_model_path, dl, False, True, map_args, tokenizer_args, tokenization_args,
                                  training_args)

res = TokenClassification.evaluate(pretrained_model_path, dl, False, map_args, tokenizer_args, tokenization_args,
                                  training_args)
