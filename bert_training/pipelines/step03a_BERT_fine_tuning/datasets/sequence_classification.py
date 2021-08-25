from datasets import load_dataset
import numpy as np


def filter_map_args(**kwargs):
    possible_args = {'with_indices', 'input_columns', 'batched', 'batch_size', 'drop_last_batch', 'remove_columns',
                     'keep_in_memory', 'load_from_cache_file', 'cache_file_name', 'writer_batch_size', 'features',
                     'disable_nullable', 'fn_kwargs', 'num_proc', 'suffix_template', 'new_fingerprint', 'desc'}
    return {k: kwargs[k] for k in kwargs if k in possible_args}


def filter_tokenization_args(**kwargs):
    possible_args = {'text_pair', 'add_special_tokens', 'padding', 'truncation', 'max_length', 'stride',
                     'is_split_into_words', 'pad_to_multiple_of', 'return_tensors', 'return_token_type_ids',
                     'return_attention_mask', 'return_overflowing_tokens', 'return_special_tokens_mask',
                     'return_offsets_mapping', 'return_length', 'verbose'}
    return {k: kwargs[k] for k in kwargs if k in possible_args}


def encode_sequence_classification_dataset(data_file_path, tokenizer, text_col="text", y_col="y",
                                           skiprows=0, delimiter="\t", **kwargs):
    dataset = load_dataset("csv", data_files=data_file_path, skiprows=skiprows, delimiter=delimiter)['train']
    classes = np.unique(dataset[y_col])

    class_to_index = {v: k for k, v in enumerate(classes)}
    index_to_class = {k: v for k, v in enumerate(classes)}

    tokenization_args = filter_tokenization_args(**kwargs)
    map_args = filter_map_args(**kwargs)

    def tokenize(batch):
        tokenized_sentences = tokenizer(text=batch[text_col], **tokenization_args)
        tokenized_sentences['label'] = [class_to_index[i] for i in batch[y_col]]
        return tokenized_sentences

    return dataset.map(tokenize, **map_args), class_to_index, index_to_class
