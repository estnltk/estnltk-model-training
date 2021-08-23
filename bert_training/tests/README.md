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

State [68f9c9f](https://gitlab.cs.ut.ee/health-informatics/medbert/-/tree/68f9c9f8216a6e41fa5a151a73c1bd5d4c69357b):

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

Some file sequence_classification.py not committed?


## TEMPORARY: Current errors in tests on UTHPC

State ecfa609, no more this error:
```
$ $ bash tests/run_quick_tests.sh
Calling: eval "$(conda shell.bash hook)"
[nltk_data] Downloading package punkt to
[nltk_data]     /gpfs/space/home/hat/nltk_data...
[nltk_data]   Unzipping tokenizers/punkt.zip.
test_NSP_dataset_creation (tests.test_pretraining_ds.PretrainingDatasetCases) ... 


100%|██████████| 1/1 [00:00<00:00,  2.17ba/s]
ok
test_span_merging_Big (tests.test_span_merge.span_testing) ... ok
test_span_merging_Edges (tests.test_span_merge.span_testing) ... ok
...
```
