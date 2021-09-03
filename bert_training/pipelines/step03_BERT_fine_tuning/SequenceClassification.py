import torch
import numpy as np

from typing import Optional, Tuple, List

from datasets import load_dataset
from sklearn.metrics import precision_recall_fscore_support, accuracy_score
from transformers import AutoTokenizer, BertForSequenceClassification, Trainer, TrainerCallback, TrainingArguments, \
    BertConfig

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
        :param train_data_paths: (str or list of str, default="") path or paths to files, that contain training data
        :param eval_data_paths: (str or list of str, default="") path or paths to files, that contain evaluation data
        :param class_to_index: (dict, default=None) a dictionary that maps classes/labels to indices
        :param text_col: (str, default="text") The column name in which the sentences are
        :param y_col: (str, default="y") The column name of the response variable
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
    def sc_compute_metrics(pred):
        labels = pred.label_ids
        preds = np.argmax(pred.predictions, axis=-1)
        precision, recall, f1, _ = precision_recall_fscore_support(labels, preds, average='weighted', zero_division=0)
        acc = accuracy_score(labels, preds)
        return {'accuracy': float(acc), 'precision': float(precision), 'recall': float(recall), 'f1': float(f1)}

    model = BertForSequenceClassification.from_pretrained(model_path, num_labels=len(cls_to_index))

    trainer = Trainer(
        model=model,
        args=TrainingArguments(**training_args_deprecation_fix(training_args)),
        train_dataset=ds['train'] if 'train' in ds else None,
        eval_dataset=ds['eval'] if 'eval' in ds else None,
        compute_metrics=sc_compute_metrics if compute_metrics is None else compute_metrics,
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


def encode_dataset(tokenizer, train_data_paths="", eval_data_paths="", class_to_index=None,
                   text_col="text", y_col="y", skiprows=0, delimiter="\t",
                   tokenization_args=None, map_args=None):
    """
    Tokenizes the input datasets and makes sure that labels are turned into indices
    :param tokenizer: (transformers.PreTrainedTokenize) See: https://huggingface.co/transformers/main_classes/tokenizer.html#pretrainedtokenizer
    :param train_data_paths: (str or list of str, default="") path or paths to files, that contain training data
    :param eval_data_paths: (str or list of str, default="") path or paths to files, that contain evaluation data
    :param class_to_index: (dict, default=None) a dictionary that maps classes/labels to indices
    :param text_col: (str, default="text") The column name in which the sentences are
    :param y_col: (str, default="y") The column name of the response variable
    :param skiprows: (int, default=0) skips described rows in the data files
    :param delimiter: (str, default="\t") the delimiter in the data files
    :param tokenization_args: (dict, default=None) See: https://huggingface.co/transformers/internal/tokenization_utils.html#transformers.tokenization_utils_base.PreTrainedTokenizerBase.__call__
    :param map_args: (dict, default=None) See: https://huggingface.co/docs/datasets/package_reference/main_classes.html?highlight=map#datasets.Dataset.map
    :return:
    """
    # Adding files to load into a dict
    files_to_load = {}
    if train_data_paths != "":
        files_to_load['train'] = train_data_paths
    if eval_data_paths != "":
        files_to_load['eval'] = eval_data_paths

    # If no files are provided, then return nothing
    if files_to_load == {}:
        return None, None, None

    dataset = load_dataset("csv", data_files=files_to_load, skiprows=skiprows, delimiter=delimiter)

    if class_to_index is None:
        # finding all class labels
        classes = set()
        for s in files_to_load.keys():
            for i in np.unique(dataset[s][y_col]):
                classes.add(i)
        # converting classes set to a list and sorting it so it would be deterministic
        classes = list(classes)
        classes.sort()
        classes = [str(i) for i in classes]

        # assigning an index to a class and vice versa
        class_to_index = {v: k for k, v in enumerate(classes)}
        index_to_class = {k: v for k, v in enumerate(classes)}
    else:
        index_to_class = {v: k for k, v in class_to_index.items()}

    # tokenization function
    def tokenize(batch):
        tokenized_sentences = tokenizer(text=batch[text_col], **tokenization_args)
        tokenized_sentences['label'] = [class_to_index[str(i)] for i in batch[y_col]]
        return tokenized_sentences

    return dataset.map(tokenize, remove_columns=[text_col, y_col], **map_args), class_to_index, index_to_class
