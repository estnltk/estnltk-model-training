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

On Windows following seems to work:

```
(medbert) $ setx RUN_SLOW_TESTS "1" && python -m unittest discover -vvv
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

State [adc68934](https://gitlab.cs.ut.ee/health-informatics/medbert/-/tree/adc68934ef07ca6679498bc0bab25fddca5fae4b):

```
$ python -m unittest discover -vvv
...
test_vocabulary_on_text (tests.test_vocabulary.textCleaningTestsCases) ... [2, 8, 6, 17, 56, 92, 63, 63, 112, 75, 106, 41, 112, 120, 94, 56, 168, 121, 116, 66, 64, 7, 19, 3]
[CLS] <XXX> <FLOAT>, võrreldes eelmise visiidiga <DATE>. [SEP]
ok

======================================================================
ERROR: tests.test_step02_eval (unittest.loader._FailedTest)
----------------------------------------------------------------------
ImportError: Failed to import test module: tests.test_step02_eval
Traceback (most recent call last):
  File "/Users/ha/miniconda3/envs/medbert/lib/python3.8/unittest/loader.py", line 436, in _find_test_path
    module = self._get_module_from_name(name)
  File "/Users/ha/miniconda3/envs/medbert/lib/python3.8/unittest/loader.py", line 377, in _get_module_from_name
    __import__(name)
  File "/Users/ha/Repos/repos-cs-ut/medbert/tests/test_step02_eval.py", line 6, in <module>
    from pipelines.step02_BERT_pre_training.eval_BERT import eval_pretrained_BERT
  File "/Users/ha/Repos/repos-cs-ut/medbert/pipelines/step02_BERT_pre_training/eval_BERT.py", line 2, in <module>
    from sklearn.metrics import precision_recall_fscore_support, accuracy_score
ModuleNotFoundError: No module named 'sklearn'


----------------------------------------------------------------------
Ran 28 tests in 44.687s

FAILED (errors=1, skipped=3)
```

Some file sequence_classification.py not committed?


## TEMPORARY: Current errors in tests on UTHPC

Currently, no UTHPC specific errors (shall test again and run all tests; note sure about EstNLT punkt.zip caching
and Huggingface caching -- how to reset those and whether they affect something or not).

```
...
```
