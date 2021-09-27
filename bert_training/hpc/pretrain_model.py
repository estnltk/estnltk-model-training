from transformers import BertConfig
from pipelines.step02_BERT_pre_training.pre_train_BERT import pre_train_BERT
import configparser

from os import listdir
from os.path import isfile, join

import os

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + '/' + 'run_config.ini')

model_path = os.path.dirname(os.path.abspath(__file__)) + '/' + config['path']['output_model_folder']
training_data_folder = os.path.dirname(os.path.abspath(__file__)) + '/' + config['path']['pretraining_data_folder']

training_args = {
    "output_dir": model_path,
    "overwrite_output_dir": True,
    "num_train_epochs": 1,
    "per_device_train_batch_size": 8,
    "per_device_eval_batch_size": 8
}


special_tokens = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"]
additional_special_tokens = ["<INT>", "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>", "<br>"]
training_files = [training_data_folder + "/" + f for f in listdir(training_data_folder) 
                  if isfile(join(training_data_folder, f))]

# just a sample config. These params arent usually used.
config = BertConfig(
    hidden_size=480,
    max_position_embeddings=1024,
    vocab_size=1234,
    model_type='bert'
)

# Vocab size and max_length are very low to ensure the pipeline works, could be added to conf.
# For now, change when needed.
pre_train_BERT(model_path,
               training_files,
               training_args=training_args, bert_config=config,
               vocab_size=800, lowercase=False, max_length=8, truncation=True, padding=True,
               special_tokens=special_tokens, additional_special_tokens=additional_special_tokens)
