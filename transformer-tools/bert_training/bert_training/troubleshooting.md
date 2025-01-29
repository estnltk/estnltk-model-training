# Some problems and solutions when training models

## NE-tags:  "xxx seems not to be NE tag"

The data consisted of sentences about patient info and possible drug reactions. The words leading up to the side-effect were initially marked with label 1 (interesting)
and the side-effect with following words marked with 0 (zero, not interesting). Zeros were converted to O-s (letter O) but ones remained.

During trainig the model gave a warning message: "1 seems not to be NE tag".  
(Along with a zero division problem which resulted in precision, recall and f1 score being 0. This should be fixed once the tags are in the correct format.) 
Replacing ones with other names like "LC" gave the same error. 

The format that the model accepts is with prefix "B-" or "I-" (capital i). So that the first word of interesting part has a tag **B-LC** and following words up to side-effect had a tag **I-LC**. The remaining words had a tag O (letter).

For example:

word	label 

Patsiendil	B-LC

on	I-LC

olnud	I-LC

peavalu	O

peale	O

ravimi	O

võtmist	O

.	O


The "B-" is for beginning of the interesting part and "I-" refers to inside of the part.

Tags in this format do not give warnings during training.

The data was saved in a tsv file. After each sentence end there should be an empty line before the first word of the next sentence.



## Numerical/probability scores from BERT models 

When using trainer, there is an option to use trainer.predict() which would give probability scores.

When a model is loaded from a checkpoint, the predict() does not work and for some models, getting logits during individual sentence prediction freezes and crashes the script.

One possible solution is to use transformers pipeline:

    from transformers import pipeline

    pipe = pipeline("ner", model=model, tokenizer=tokenizer)
	
    sample_text = "something something something"

    result = pipe(sample_text) 

The "ner" in pipeline is a task, which can also be "text-classification". Please see https://huggingface.co/docs/transformers/main_classes/pipelines. 
The model can be "my_model.model" or a path to folder "path/to/my/model/" which contains files from training (if after trainer.train() the model is not specifically saved as .model).


The result will be a list of dictionaries. The dictionary will contain a key 'score' which is the probability needed for calculating precision-recall curve etc.

Side note: giving sample texts in str format one by one works fine. Giving Dataset object will create problems with tokenization as tensor a length will not match tensor b length.

	ds = Dataset("csv", {"train":path/to/tsv}, "\t") # train will consist of texts and labels

	result = pipe(ds["train"]["text"])  # results in "tensor a length does not match tensor b" error




