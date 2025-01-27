import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '4' 


import pickle
from transformers import AutoTokenizer
import torch
import torch.nn as nn
from transformers import Trainer, TrainingArguments
import pandas as pd
import json

from bert_phrase_removal_head import BertForPhraseRelevance
from custom_dataset import PhraseRemovalDataset

import matplotlib.pyplot as plt
import numpy as np

from sklearn.metrics import accuracy_score, auc, average_precision_score, confusion_matrix, roc_curve, precision_recall_curve


def test_model(rmodel, tests): 
    
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    
    probs = []
    recalls = []
    precs = []
    pred_labels=[]
    
    preds = []
    true_labels = []
    
    for i,row in enumerate(tests.iterrows()):
        verb = tests.iloc[i]["verb"]
        phrase = tests.iloc[i]["removed"]
        real_label = tests.iloc[i]["label"]
        
        combined_input = f"{phrase} [SEP] {verb}"
        
        # Tokenize and pad sequences to the same length
        inputs = tokenizer.encode_plus( combined_input,
                                        add_special_tokens=True,
                                        max_length=512,
                                        padding='max_length',
                                        truncation=True,
                                        return_attention_mask=True,
                                        return_tensors='pt'
                                        )

        outputs = rmodel(**inputs)
        logits = outputs[1]
        
        #predicts = rmodel.predict(tests)
        probs = torch.nn.functional.softmax(logits, dim=1)
        pred = torch.argmax(probs, dim=-1).item()
        prob_score = probs[:, 1].item()
        
        #answer = "yes" if torch.argmax(logits) == 1 else "no"
        pred = torch.argmax(logits)
        if pred == real_label: # prediction matches real label
            if pred == 0: # predicted negative
                tn+=1
            else:
                tp +=1
        else: # prediction doesn't match real label
            if pred == 0: # predicted false, real is true
                fn +=1 
            else:
                fp += 1

        preds.append(logits.detach()[0][0])
        pred_labels.append(pred)
        true_labels.append(real_label)
        
        precision = tp/(tp+fp) if tp+fp > 0 else 0
        recall = tp/(tp+fn) if tp+fn > 0 else 0
        precs.append(precision)
        recalls.append(recall)

    #print(f'precision:{round((tp/(tp+fp))*100, 1)}, recall:{round((tp/(tp+fn))*100, 1)}')
    #print(f'stats: tp:{tp}  tn:{tn}  fp:{fp}  fn:{fn}')
    
    return precs, recalls, preds, true_labels, pred_labels #, tp, fp, tn, fn




tokenizer = AutoTokenizer.from_pretrained('EMBEDDIA/est-roberta')


recalls = []
precs = []
predictions = []
test_labels = []
predicted_labels = []


tests = pd.read_csv(f"data/obl_gpt_large1.csv", sep=';', encoding='utf-8')
tmodel = BertForPhraseRelevance.from_pretrained(f'.../model_training/obl_train_v1/e20b16lr3e6w000001/checkpoint-1183')
prec, rec, pred, label, pred_label = test_model(tmodel, tests)
recalls.append(rec)
precs.append(prec)
predictions+=pred
test_labels+=label
predicted_labels+=pred_label

result_dict = {"recalls":recalls, "precs":precs, "predictions":predictions, "test_labels":test_labels, "predicted_labels":predicted_labels}

pickle.dump(result_dict, open("results/obl_train_v1_e20b16lr3e6w000001_cp1183.txt",'wb'))













