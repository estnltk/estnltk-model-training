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

## Run quick tests on UTHPC

```
[username@rocket medbert]$ bash tests/run_quick_tests.sh
```

This tail-follows SLURM output for you.


## TEMPORARY: Current errors locally

```
$ python -m unittest discover -vvv
...
Ran 36 tests in 71.525s

OK (skipped=11)
```

No errors locally with non-slow tests.


## TEMPORARY: Current errors in tests on UTHPC

Getting git state in use: `git rev-parse --short HEAD`.

Currently, via running quick test like so `bash tests/run_quick_tests.sh` at `9500360`:

```
loading file https://huggingface.co/bert-base-cased/resolve/main/tokenizer_config.json from cache at /gpfs/space/home/hat/.cache/huggingface/transformers/ec84e86ee39bfe112543192cf981deebf7e6cbe8c91b8f7f8f63c9be44366158.ec5c189f89475aac7d8cbd243960a0655cfadc3d0474da8ff2ed0bf1699c2a5f
100%|██████████| 1/1 [00:02<00:00,  2.06s/ba]
ok
test_finetuning_sequence_classification_model (tests.test_step03_pipelines.TextCleaningTestsCases) ... skipped 'Warning! These tests are slow, since training is slow'
test_next_line_symbol_swap (tests.test_text_cleaning.textCleaningTestsCases) ... ok
test_text_cleaning_clean (tests.test_text_cleaning.textCleaningTestsCases) ... ok
test_text_cleaning_date (tests.test_text_cleaning.textCleaningTestsCases) ... ok
test_text_cleaning_event_header (tests.test_text_cleaning.textCleaningTestsCases) ... ok
test_text_cleaning_event_numbers (tests.test_text_cleaning.textCleaningTestsCases) ... ok

======================================================================
FAIL: test_pre_trained_model_eval (tests.test_step02_eval.PretrainedModelEvaluation)
----------------------------------------------------------------------
Traceback (most recent call last):
  File "/gpfs/space/home/hat/projects/medbert-run/medbert/tests/test_step02_eval.py", line 39, in test_pre_trained_model_eval
    self.assertEqual(round(m[k], 4), round(m_exp[k], 4), f"mlm At {k}")
AssertionError: 5.3845 != 4.9631 : mlm At eval_loss

----------------------------------------------------------------------
Ran 28 tests in 380.494s

FAILED (failures=1, skipped=3)
WARNING:builder.py:383: Using custom data configuration default-be2972f20564b0db

```

---

ALSO: Note sure about EstNLT punkt.zip caching and Huggingface caching -- 
      how to reset those and whether they affect something or not.
