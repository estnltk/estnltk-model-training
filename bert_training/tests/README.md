# Tests - How to run and what to expect

## Run all tests except slow ones

Slow tests are skipped by default, others can be run like so:

```
(medbert) $ python -m unittest discover -vvv
```

Should take less than a minute.


## Run all tests, including slow ones

NB! Running all tests locally (with non-GPU machine) works in principle but takes a long time, 
    about 2h 15min on '2,9 GHz Quad-Core Intel Core i7, 16 GB 2133 MHz LPDDR3'-powered machine.
    Time-consuming is CPU-based training in [test_step02_pipeline.py](test_step02_pipeline.py).

You can do it like so:

```
(medbert) $ RUN_SLOW_TESTS=1 python -m unittest discover -vvv
```


## Running certain specific pipeline / test file / test cases

Example:

```
(medbert) $ python -m unittest tests/test_step01_pipelines.py
```

or running tests in some TestCase class:

```
(medbert) $ python -m unittest tests.test_pretraining_ds.PretrainingDatasetCases
```


## Run one particular test

Example:

```
(medbert) $ python -m unittest tests.test_step01_pipelines.TextCleaningTestsCases.test_tsv_to_bert_input_pipeline_clean_par
```


## TEMPORARY: Current errors in tests on UTHPC

```
$ cat slurm-19906010.out
Calling: eval "$(conda shell.bash hook)"
[nltk_data] Downloading package punkt to
[nltk_data]     /gpfs/space/home/hat/nltk_data...
[nltk_data]   Unzipping tokenizers/punkt.zip.
test_tokenizer (tests.test_pretraining_ds.PretrainingDatasetCases) ... ERROR
test_span_merging_Big (tests.test_span_merge.span_testing) ... ok
...
test_text_cleaning_event_numbers (tests.test_text_cleaning.textCleaningTestsCases) ... ok
test_vocabulary_creation_unigram (tests.test_vocabulary.textCleaningTestsCases) ... 


ok
test_vocabulary_on_text (tests.test_vocabulary.textCleaningTestsCases) ... ok

======================================================================
ERROR: test_tokenizer (tests.test_pretraining_ds.PretrainingDatasetCases)
----------------------------------------------------------------------
Traceback (most recent call last):
  File "/gpfs/space/home/hat/projects/medbert-run/medbert/tests/test_pretraining_ds.py", line 37, in test_tokenizer
    self.create_test_vocab(model_path, input)
  File "/gpfs/space/home/hat/projects/medbert-run/medbert/tests/test_pretraining_ds.py", line 29, in create_test_vocab
    create_vocabulary(model_path, train_files, size, special_tokens=special_tokens)
  File "/gpfs/space/home/hat/projects/medbert-run/medbert/pipelines/step02_BERT_pre_training/tokenizing/vocabulary_creator.py", line 23, in create_vocabulary
    tokenizer.train(files, vocab_size=size, special_tokens=special_tokens)
  File "/gpfs/space/home/hat/.conda/envs/medbert/lib/python3.8/site-packages/tokenizers/implementations/bert_wordpiece.py", line 118, in train
    self._tokenizer.train(files, trainer=trainer)
Exception: No such file or directory (os error 2)

----------------------------------------------------------------------
Ran 25 tests in 309.634s

FAILED (errors=1, skipped=2)
[2, 8, 6, 17, 56, 84, 69, 69, 114, 67, 106, 41, 114, 129, 56, 108, 118, 113, 78, 66, 7, 19, 3]
[CLS] <XXX> <FLOAT>, võrreldes eelmise visiidiga <DATE>. [SEP]
```
