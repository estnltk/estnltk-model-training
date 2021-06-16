# Pipelines

## text_collection_processer.py

Cleans and turns text collections into form that is ready to be tokenized
Inputs:
 * Text files
 * EstNLTK Text objects or collections
 * Standard text corpuses

Results:
 * Text file? that is ready to be tokenized

## pre_train_BERT.py

Uses the texts processed in the previous step to:
 * Create a vocabulary (if not provided)
 * Tokenize the texts (if not provided | save if required)
 * Tokenize the texts (if not provided | save if required)
 * pre-train BERT

While pre-training, it also displays diagnostics (MLM and NSP scores)

## fine_tune_BERT.py

Fine-tunes a BERT model

Inputs:
 * tagged text files
 * tagged EstNLTK collections

## fine_tune_specific_BERT.py

Fine-tunes a specific BERT model

Inputs:
 * Task name
 * pre-processed data
