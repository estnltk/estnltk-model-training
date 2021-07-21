import random

from datasets import load_dataset, load_metric
from transformers import BertConfig, DataCollatorForLanguageModeling, TrainingArguments, Trainer, \
    BertForPreTraining, BertTokenizerFast
import numpy as np

model_path = "C:/Users/Meelis/PycharmProjects/medbert/data/test_model/"

data_path = "C:/Users/Meelis/PycharmProjects/medbert/data/corp_res_clean_r_events_exp.tsv"
dataset = load_dataset("csv", data_files={'train': data_path})['train']
additional_special_tokens = ["<INT>", "<FLOAT>", "<DATE>", "<XXX>", "<ADJ>", "<NAME>", "<ADV>", "<INJ>"]
tokenizer = BertTokenizerFast.from_pretrained("C:/Users/Meelis/PycharmProjects/medbert/data/test_model/",
                                              do_lower_case=False,
                                              additional_special_tokens=additional_special_tokens)
config = BertConfig()
config.vocab_size = 3000
nsp_probability = 0.5
model = BertForPreTraining(config)
print(dataset[1])
"""
def tokenize_and_encode(batch):
    for i in range(len(batch) - 1):
        pass

    if random.random() <= nsp_probability:
        pass

    return tokenizer(batch['text'], truncation=True)
"""

# dataset_enc = dataset.map(tokenize_and_encode, batched=True)
accuracy_score = load_metric("accuracy")
"""
def compute_metrics(eval_pred):
    predictions, labels = eval_pred
    predictions = np.argmax(predictions, axis=1)
    return accuracy_score.compute(predictions=predictions, references=labels)




data_collator = DataCollatorForLanguageModeling(
    tokenizer=tokenizer,
    mlm=True,
    mlm_probability=0.15
)

training_args = TrainingArguments(
    output_dir=model_path + "out",
    overwrite_output_dir=True,
    num_train_epochs=1,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8
)

trainer = Trainer(
    model=model,
    args=training_args,
    compute_metrics=compute_metrics,
    data_collator=data_collator,
    train_dataset=dataset_enc,
    tokenizer=tokenizer
)

trainer.train()
trainer.save_model(model_path)
"""
