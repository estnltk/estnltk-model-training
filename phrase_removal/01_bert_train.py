import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '4' 

from transformers import BertTokenizer, BertModel, BertPreTrainedModel, BertForMultipleChoice, AutoTokenizer
import torch
import torch.nn as nn
from transformers import Trainer, TrainingArguments
import pandas as pd
from torch.utils.data import Dataset, DataLoader
import numpy as np

from bert_phrase_removal_head import BertForPhraseRelevance
from custom_dataset import PhraseRemovalDatasetVerb

from transformers import EarlyStoppingCallback
from sklearn.metrics import accuracy_score, recall_score, precision_score, f1_score


from torch import cuda
device = 'cuda' if cuda.is_available() else 'cpu'


def compute_metrics(p):    
    pred, labels = p
    pred = np.argmax(pred, axis=1)
    accuracy = accuracy_score(y_true=labels, y_pred=pred)
    recall = recall_score(y_true=labels, y_pred=pred)
    precision = precision_score(y_true=labels, y_pred=pred)
    f1 = f1_score(y_true=labels, y_pred=pred)    
    return {"accuracy": accuracy, "precision": precision, "recall": recall, "f1": f1}





tokenizer = AutoTokenizer.from_pretrained('EMBEDDIA/est-roberta')
model = BertForPhraseRelevance.from_pretrained('EMBEDDIA/est-roberta')

model.to(device)

dataset = PhraseRemovalDatasetVerb(tokenizer, 'data/obl_non_gpt_large1.csv', max_length=512)
eval_dataset = PhraseRemovalDatasetVerb(tokenizer, 'data/obl_non_gpt_large1_eval.csv', max_length=512)

# Define the training arguments
training_args = TrainingArguments(
                output_dir='./model_training/obl_train_v1/e20b16lr1e6w000001',          # Output directory
                num_train_epochs=20,              # Total number of training epochs
                per_device_train_batch_size=16,  # Batch size per device during training
                warmup_steps=int((len(dataset)/16*20)*0.1),                # Number of warmup steps for learning rate scheduler
                weight_decay=0.000001,               # Strength of weight decay
                logging_dir='./model_training/obl_train_v1/e20b16lr1e6w000001',            # Directory for storing logs
                logging_steps=100,
                save_strategy='epoch', 
                learning_rate=1e-6,
                #label_names=['labels'],
                evaluation_strategy = 'epoch',
                load_best_model_at_end=True,
                metric_for_best_model = 'precision'
                )

# Initialize the Trainer
trainer = Trainer(
    model=model,                         # The instantiated Transformers model to be trained
    args=training_args,                  # Training arguments, defined above
    train_dataset=dataset,               # Training dataset
    eval_dataset=eval_dataset,   # Optional: evaluation dataset
    callbacks = [EarlyStoppingCallback(early_stopping_patience=20)],
    compute_metrics=compute_metrics,
    )


# Start the training
trainer.train()



