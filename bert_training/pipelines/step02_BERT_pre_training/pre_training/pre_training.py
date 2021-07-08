from transformers import BertTokenizer, BertConfig, DataCollatorForLanguageModeling, \
    TextDatasetForNextSentencePrediction, TrainingArguments, Trainer, BertForPreTraining

model_path = "C:/Users/Meelis/PycharmProjects/medbert/data/test_model/"
vocab_path = "C:/Users/Meelis/PycharmProjects/medbert/data/test_model/vocab.txt"
data_path = "C:/Users/Meelis/PycharmProjects/medbert/data/corp_res_clean_r_events_exp.txt"
tokenizer = BertTokenizer(vocab_path)
config = BertConfig()
config.vocab_size = 3000
model = BertForPreTraining(config)

dataset = TextDatasetForNextSentencePrediction(
    tokenizer=tokenizer,
    file_path=data_path,
    block_size=256
)
data_collator = DataCollatorForLanguageModeling(
    tokenizer=tokenizer,
    mlm=True,
    mlm_probability=0.15
)

training_args = TrainingArguments(
    output_dir=model_path + "out",
    overwrite_output_dir=True,
    num_train_epochs=2,
    per_gpu_train_batch_size=8,
    save_steps=1000,
    save_total_limit=2,
    prediction_loss_only=True,
)

trainer = Trainer(
    model=model,
    args=training_args,
    data_collator=data_collator,
    train_dataset=dataset,
)

trainer.train()
trainer.save_model(model_path)
