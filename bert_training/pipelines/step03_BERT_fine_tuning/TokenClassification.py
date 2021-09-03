import csv
from typing import Optional, Tuple, List

import datasets
import numpy as np
import torch
from datasets import Dataset
from sklearn.metrics import precision_recall_fscore_support, accuracy_score
from transformers import TrainerCallback, AutoTokenizer, BertConfig, BertForTokenClassification, Trainer, \
    TrainingArguments

from pipelines.step02_BERT_pre_training.pre_training.Helpers import training_args_deprecation_fix


def finetune_BERT(model_path, save_model=True, dataset_args=None, map_args=None, tokenizer_args=None,
                  tokenization_args=None, training_args=None, compute_metrics=None,
                  callbacks: Optional[List[TrainerCallback]] = None,
                  optimizers: Tuple[torch.optim.Optimizer, torch.optim.lr_scheduler.LambdaLR] = (None, None)):
    """
    Fine-tunes a bert model on a sequence classification task
    :param model_path: (str) Path to the pretrained BERT model
    :param save_model: (bool, default=True) To save a model or not
    :param dataset_args: (dict, default=None) A dictionary that can contain:
        :param tokenizer: (transformers.PreTrainedTokenize) See: https://huggingface.co/transformers/main_classes/tokenizer.html#pretrainedtokenizer
        :param train_data_paths: (str or list of str, default="") path or paths to files, that contain training data
        :param eval_data_paths: (str or list of str, default="") path or paths to files, that contain evaluation data
        :param label_all_subword_units: (bool, default=False) if set to True, the function labels all subword units, if False, then only the first
        :param class_to_index: (dict, default=None) a dictionary that maps classes/labels to indices
        :param has_header_col: (bool, default=True) Set to True if the column names are in the dataset. If False then set text_col and y_col as integers!
        :param text_col: (str or int, default="text") The column name or index in which the sentences are
        :param y_col: (str or int, default="y") The column name or index of the response variable
        :param skiprows: (int, default=0) skips described rows in the data files
        :param delimiter: (str, default="\t") the delimiter in the data files
    :param map_args: (dict, default=None) See: https://huggingface.co/docs/datasets/package_reference/main_classes.html?highlight=map#datasets.Dataset.map
    :param tokenizer_args: (dict, default=None) See: https://huggingface.co/transformers/model_doc/bert.html#berttokenizerfast
    :param tokenization_args: (dict, default=None) See: https://huggingface.co/transformers/internal/tokenization_utils.html#transformers.tokenization_utils_base.PreTrainedTokenizerBase.__call__
    :param training_args: (dict, default=None), Arguments used in training. If None, the default training
         args will be used (except do_train will be True and output_dir will be the model path). (See: https://huggingface.co/transformers/main_classes/trainer.html#transformers.TrainingArguments)
    :param compute_metrics: (function, default=None), Function that calculates metrics. If None then accuracy, precision, recall and f1 scores are calculated
    :param callbacks: (list of functions, default=None) A list of callbacks to customize the training loop. (see https://huggingface.co/transformers/main_classes/callback.html)
    :param optimizers: (A tuple containing the optimizer and the scheduler, default = (None, None)) Will default to an instance of AdamW on your model and a scheduler given by get_linear_schedule_with_warmup() controlled by args.
    :return: If eval_dataset is provided, then it returns the results of the evaluation. Otherwise None.
    """

    # dataset creation
    tokenizer = AutoTokenizer.from_pretrained(model_path, **tokenizer_args)

    # if class to index is not provided, then try to load it from the config
    if "class_to_index" not in dataset_args or dataset_args['dataset_args'] is None:
        config = BertConfig.from_pretrained(model_path)
        if "label2id" in config.to_dict().keys() and config.label2id != {'LABEL_0': 0, 'LABEL_1': 1}:
            dataset_args['class_to_index'] = config.label2id

    # preparing the dataset for sequence classification
    ds, cls_to_index, index_to_cls = encode_dataset(tokenizer,
                                                    tokenization_args=tokenization_args,
                                                    map_args=map_args,
                                                    **dataset_args)
    if ds is None:
        raise Exception("No dataset was provided")

    # a metric for sequence classification
    # can be replaced by providing a new function using the compute_metrics argument
    def tc_compute_metrics(p):
        predictions, labels = p
        predictions = np.argmax(predictions, axis=2)

        # Remove ignored index (special tokens)
        true_predictions = []
        true_labels = []
        for prediction, label in zip(predictions, labels):
            for (p, l) in zip(prediction, label):
                if l != -100:
                    true_predictions.append(index_to_cls[p])
                    true_labels.append(index_to_cls[l])

        precision, recall, f1, _ = precision_recall_fscore_support(true_labels, true_predictions, average='weighted',
                                                                   zero_division=0)
        acc = accuracy_score(true_labels, true_predictions)
        return {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "accuracy": acc,
        }

    model = BertForTokenClassification.from_pretrained(model_path, num_labels=len(cls_to_index))

    trainer = Trainer(
        model=model,
        args=TrainingArguments(**training_args_deprecation_fix(training_args)),
        train_dataset=ds['train'] if 'train' in ds else None,
        eval_dataset=ds['eval'] if 'eval' in ds else None,
        compute_metrics=tc_compute_metrics if compute_metrics is None else compute_metrics,
        callbacks=callbacks,
        optimizers=optimizers,
    )

    if 'train' in ds:
        trainer.train()

    if save_model:
        trainer.model.config.id2label = index_to_cls
        trainer.model.config.label2id = cls_to_index
        trainer.save_model()
        tokenizer.save_pretrained(training_args['output_dir'])

    if 'eval' in ds:
        return trainer.evaluate()


def encode_dataset(tokenizer, train_data_paths="", eval_data_paths="", has_header_col=True, text_col="text", y_col="y",
                   label_all_subword_units=False, class_to_index=None, skiprows=0, delimiter="\t",
                   tokenization_args=None, map_args=None):
    """
    Tokenizes the input datasets and makes sure that labels are turned into indices
    :param tokenizer: (transformers.PreTrainedTokenize) See: https://huggingface.co/transformers/main_classes/tokenizer.html#pretrainedtokenizer
    :param train_data_paths: (str or list of str, default="") path or paths to files, that contain training data
    :param eval_data_paths: (str or list of str, default="") path or paths to files, that contain evaluation data
    :param label_all_subword_units: (bool, default=False) if set to True, the function labels all subword units, if False, then only the first
    :param class_to_index: (dict, default=None) a dictionary that maps classes/labels to indices
    :param has_header_col: (bool, default=True) Set to True if the column names are in the dataset. If False then set text_col and y_col as integers!
    :param text_col: (str or int, default="text") The column name or index in which the sentences are
    :param y_col: (str or int, default="y") The column name or index of the response variable
    :param skiprows: (int, default=0) skips described rows in the data files
    :param delimiter: (str, default="\t") the delimiter in the data files
    :param tokenization_args: (dict, default=None) note: is_split_into_words is set to True, original default = False". See: https://huggingface.co/transformers/internal/tokenization_utils.html#transformers.tokenization_utils_base.PreTrainedTokenizerBase.__call__
    :param map_args: (dict, default=None) See: https://huggingface.co/docs/datasets/package_reference/main_classes.html?highlight=map#datasets.Dataset.map
    :return:
    """

    # token rows into sentences
    def _read_BIO_format(file_path):
        sentences = []
        labels = []
        tokens = []
        labs = []
        with open(file_path, encoding="utf-8") as input_file:
            reader = csv.reader(input_file, delimiter=delimiter)

            if has_header_col and isinstance(text_col, str) and isinstance(y_col, str):
                tok_i, lab_i = resolve_ds_col_ids(next(reader), text_col, y_col)
            elif isinstance(text_col, int) and isinstance(y_col, int):
                tok_i, lab_i = text_col, y_col
            else:
                raise ValueError("Invalid combination of has_header_col, text_col and y_col")


            for _ in range(skiprows):
                next(reader)
            for row in reader:
                if row[0] == '':
                    sentences.append(tokens)
                    labels.append(labs)
                    tokens = []
                    labs = []
                else:
                    tokens.append(row[tok_i])
                    labs.append(row[lab_i])

        return sentences, labels

    def _build_dict(data_paths):
        if not isinstance(data_paths, list):
            data_paths = [data_paths]
        ds = {"X": [], "Y": []}
        if data_paths[0] == "":
            return ds

        for path in data_paths:
            x, y = _read_BIO_format(path)
            ds['X'].extend(x)
            ds['Y'].extend(y)
        return ds

    train = _build_dict(train_data_paths)
    evl = _build_dict(eval_data_paths)

    train_dataset = Dataset.from_dict(train)
    eval_dataset = Dataset.from_dict(evl)

    dataset = datasets.DatasetDict({"train": train_dataset, "eval": eval_dataset})

    # removing empty
    keys_to_pop = []
    for k in dataset.keys():
        if dataset[k].num_rows == 0:
            keys_to_pop.append(k)
    for k in keys_to_pop:
        dataset.pop(k)

    if class_to_index is None:
        # finding all class labels
        classes = set()
        for s in dataset.keys():
            for labels in dataset[s]['Y']:
                for lab in np.unique(labels):
                    classes.add(lab)
        # converting classes set to a list and sorting it so it would be deterministic
        classes = list(classes)
        classes.sort()
        classes = [str(i) for i in classes]

        # assigning an index to a class and vice versa
        class_to_index = {v: k for k, v in enumerate(classes)}
        index_to_class = {k: v for k, v in enumerate(classes)}
    else:
        index_to_class = {v: k for k, v in class_to_index.items()}

    # 'is_split_into_words must be True
    if 'is_split_into_words' not in tokenization_args:
        tokenization_args['is_split_into_words'] = True

    # https://github.com/huggingface/notebooks/blob/master/examples/token_classification.ipynb
    def tokenize_and_align_labels(examples):
        tokenized_inputs = tokenizer(text=examples["X"], **tokenization_args)

        labels = []
        for i, label in enumerate(examples["Y"]):
            word_ids = tokenized_inputs.word_ids(batch_index=i)
            previous_word_idx = None
            label_ids = []
            for word_idx in word_ids:
                # Special tokens have a word id that is None. We set the label to -100 so they are automatically
                # ignored in the loss function.
                if word_idx is None:
                    label_ids.append(-100)
                # We set the label for the first token of each word.
                elif word_idx != previous_word_idx:
                    label_ids.append(class_to_index[label[word_idx]])
                # For the other tokens in a word, we set the label to either the current label or -100, depending on
                # the label_all_tokens flag.
                else:
                    label_ids.append(class_to_index[label[word_idx]] if label_all_subword_units else -100)
                previous_word_idx = word_idx

            labels.append(label_ids)

        tokenized_inputs["labels"] = labels
        return tokenized_inputs

    return dataset.map(tokenize_and_align_labels, remove_columns=["X", "Y"], **map_args), class_to_index, index_to_class


def resolve_ds_col_ids(row, text_col, y_col):
    return resolve_ds_col_id(row, text_col), resolve_ds_col_id(row, y_col)


def resolve_ds_col_id(row, col):
    if isinstance(col, int):
        return col
    for i, n in enumerate(row):
        if n.strip() == col:
            return i
    return None
