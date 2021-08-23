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

## TEMPORARY: Current errors locally

Seis [68f9c9f](https://gitlab.cs.ut.ee/health-informatics/medbert/-/tree/68f9c9f8216a6e41fa5a151a73c1bd5d4c69357b):

```
$ python -m unittest discover -vvv
...
ok
test_vocabulary_on_text (tests.test_vocabulary.textCleaningTestsCases) ... [2, 8, 6, 17, 56, 81, 69, 69, 111, 73, 105, 41, 111, 75, 109, 56, 108, 118, 114, 64, 74, 7, 19, 3]
[CLS] <XXX> <FLOAT>, võrreldes eelmise visiidiga <DATE>. [SEP]
ok

======================================================================
ERROR: tests.test_step03_datasets (unittest.loader._FailedTest)
----------------------------------------------------------------------
ImportError: Failed to import test module: tests.test_step03_datasets
Traceback (most recent call last):
File "/Users/ha/miniconda3/envs/medbert/lib/python3.8/unittest/loader.py", line 436, in _find_test_path
module = self._get_module_from_name(name)
File "/Users/ha/miniconda3/envs/medbert/lib/python3.8/unittest/loader.py", line 377, in _get_module_from_name
__import__(name)
File "/Users/ha/Repos/repos-cs-ut/medbert/tests/test_step03_datasets.py", line 7, in <module>
from pipelines.step03a_BERT_fine_tuning.datasets.sequence_classification import encode_sequence_classification_dataset
ModuleNotFoundError: No module named 'pipelines.step03a_BERT_fine_tuning.datasets.sequence_classification'


======================================================================
ERROR: tests.test_step03_pipelines (unittest.loader._FailedTest)
----------------------------------------------------------------------
ImportError: Failed to import test module: tests.test_step03_pipelines
Traceback (most recent call last):
File "/Users/ha/miniconda3/envs/medbert/lib/python3.8/unittest/loader.py", line 436, in _find_test_path
module = self._get_module_from_name(name)
File "/Users/ha/miniconda3/envs/medbert/lib/python3.8/unittest/loader.py", line 377, in _get_module_from_name
__import__(name)
File "/Users/ha/Repos/repos-cs-ut/medbert/tests/test_step03_pipelines.py", line 4, in <module>
from pipelines.step03a_BERT_fine_tuning.fine_tune_BERT import finetune_BERT_model_on_sequence_classification
File "/Users/ha/Repos/repos-cs-ut/medbert/pipelines/step03a_BERT_fine_tuning/fine_tune_BERT.py", line 3, in <module>
from pipelines.step03a_BERT_fine_tuning.datasets.sequence_classification import encode_sequence_classification_dataset
ModuleNotFoundError: No module named 'pipelines.step03a_BERT_fine_tuning.datasets.sequence_classification'


----------------------------------------------------------------------
Ran 27 tests in 36.046s

FAILED (errors=2, skipped=2)
```

Vist mingi fail sequence_classification.py commitimata?


## TEMPORARY: Current errors in tests on UTHPC

Harry: Mul on tihtipeale selle EstNLTK ja punkt.zip-iga alati mingi häda. Ennevanasti on olnud õiguste probleeme.

Näen, et (Est)NLTK tekitab struktuuri:

```
/gpfs/space/home/{username}/nltk_data
    tokenizers/
        punkt/
            PY3/
                README
                czech.pickle
                danish.pickle
                dutch.pickle
                english.pickle
                estonian.pickle
                finnish.pickle
                french.pickle
                german.pickle
                greek.pickle
                italian.pickle
                norwegian.pickle
                polish.pickle
                portuguese.pickle
                russian.pickle
                slovene.pickle
                spanish.pickle
                swedish.pickle
                turkish.pickle
            README
            czech.pickle
            danish.pickle
            dutch.pickle
            english.pickle
            estonian.pickle
            finnish.pickle
            french.pickle
            german.pickle
            greek.pickle
            italian.pickle
            norwegian.pickle
            polish.pickle
            portuguese.pickle
            russian.pickle
            slovene.pickle
            spanish.pickle
            swedish.pickle
            turkish.pickle
        punkt.zip
```

Vead testidega (seisus [4b261dc](https://gitlab.cs.ut.ee/health-informatics/medbert/-/tree/4b261dcd566f40c60a4d23a3dabcfcc3792b85df)):
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
