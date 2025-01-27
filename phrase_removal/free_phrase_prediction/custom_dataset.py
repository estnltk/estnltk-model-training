
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import torch
import torch.nn as nn
import pandas as pd
from torch.utils.data import Dataset, DataLoader


class PhraseRemovalDataset(Dataset):
    def __init__(self, tokenizer, filepath, max_length=512):
        self.tokenizer = tokenizer
        self.data = pd.read_csv(filepath, encoding='utf-8', sep=';')
        self.max_length = max_length

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        sentence = self.data.iloc[idx]['sentence']
        phrase = self.data.iloc[idx]['phrase']
        label = torch.tensor(self.data.iloc[idx]['label'], dtype=torch.long)

        # Tokenize and encode the sentence
        sentence_encoding = self.tokenizer.encode_plus(
                                                sentence,
                                                truncation=True,
                                                return_attention_mask=True,
                                                return_tensors='pt'
                                                )

        # Tokenize the phrase and find its position in the sentence encoding
        phrase_tokens = self.tokenizer.tokenize(phrase)
        phrase_ids = self.tokenizer.convert_tokens_to_ids(phrase_tokens)
        
        input_ids = sentence_encoding['input_ids'].squeeze().tolist()
        phrase_start = -1
        for i in range(len(input_ids) - len(phrase_ids) + 1):
            if input_ids[i:i + len(phrase_ids)] == phrase_ids:
                phrase_start = i
                break        
        
        if phrase_start == -1:
            raise ValueError(f"Phrase '{phrase}' not found in sentence '{sentence}'")

        phrase_end = phrase_start + len(phrase_ids)

        # Extract the phrase IDs from the sentence encoding
        phrase_ids_from_sentence = input_ids[phrase_start:phrase_end]

        # Combine sentence IDs and phrase IDs with [SEP] token
        sep_token_id = self.tokenizer.convert_tokens_to_ids('[SEP]')
        combined_ids = input_ids + [sep_token_id] + phrase_ids_from_sentence

        
        # Pad the combined IDs to max_length
        combined_ids = combined_ids[:self.max_length]
        attention_mask = [1] * len(combined_ids)
        padding_length = self.max_length - len(combined_ids)
        if padding_length > 0:
            combined_ids += [1] * padding_length
            attention_mask += [0] * padding_length
        
        return {
                'input_ids': torch.tensor(combined_ids), 
                'attention_mask': torch.tensor(attention_mask), 
                'labels': label
                }


class PhraseRemovalDatasetBase(Dataset):
    def __init__(self, tokenizer, filepath, max_length=512):
        self.tokenizer = tokenizer
        self.data = pd.read_csv(filepath, encoding='utf-8', sep=';')
        self.max_length = max_length

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        sentence = self.data.iloc[idx]['text']
        phrase = self.data.iloc[idx]['phrase']
        label = torch.tensor(self.data.iloc[idx]['label'], dtype=torch.long)

        # Tokenize and pad sequences to the same length
        inputs = self.tokenizer.encode_plus(
                                        sentence+"[SEP]"+phrase,
                                        add_special_tokens=True,
                                        max_length=self.max_length,
                                        padding='max_length',
                                        truncation=True,
                                        return_attention_mask=True,
                                        return_tensors='pt'
                                        )

        # Make sure that the 'input_ids' tensor is the correct size
        inputs['input_ids'] = inputs['input_ids'][:, :self.max_length]
        #inputs['labels'] = label

        #return inputs
        
        return {
                'input_ids': inputs['input_ids'].squeeze(),
                'attention_mask': inputs['attention_mask'].squeeze(),
                'labels': label
                }



class PhraseRemovalDatasetVerb(Dataset):
    def __init__(self, tokenizer, filepath, max_length=512):
        self.tokenizer = tokenizer
        self.data = pd.read_csv(filepath, encoding='utf-8', sep=';')
        self.max_length = max_length

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        verb = self.data.iloc[idx]['verb']
        phrase = self.data.iloc[idx]['removed']
        label = torch.tensor(self.data.iloc[idx]['label'], dtype=torch.long)

        # Combine phrase and verb with [SEP] token
        combined_input = f"{phrase} [SEP] {verb}"
        
        # Tokenize and pad sequences to the same length
        inputs = self.tokenizer.encode_plus(
                                        combined_input,
                                        add_special_tokens=True,
                                        max_length=self.max_length,
                                        padding='max_length',
                                        truncation=True,
                                        return_attention_mask=True,
                                        return_tensors='pt'
                                        )

        # Make sure that the 'input_ids' tensor is the correct size
        inputs['input_ids'] = inputs['input_ids'][:, :self.max_length]

        return {
                'input_ids': inputs['input_ids'].squeeze(),
                'attention_mask': inputs['attention_mask'].squeeze(),
                'labels': label
                }





