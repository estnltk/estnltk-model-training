from transformers import AutoTokenizer

from pipelines.step03a_BERT_fine_tuning.datasets.sequence_classification import encode_sequence_classification_dataset


def finetune_BERT_model_on_sequence_classification(model_path, train_data_path, test_data_path="", eval_data_path="",
                                                   text_col="text", y_col="category", **kwargs):
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased", **kwargs)
    ds, a, b = encode_sequence_classification_dataset(train_data_path, tokenizer, text_col=text_col, y_col=y_col, **kwargs)

