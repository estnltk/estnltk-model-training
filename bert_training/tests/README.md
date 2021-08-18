# Tests - How to run and what to expect

## Run all tests

```
(medbert) $ python -m unittest discover -vvv
```

NB! Running all tests locally (with non-GPU machine) works in principle but takes a long time, 
    about 2h 15min on '2,9 GHz Quad-Core Intel Core i7, 16 GB 2133 MHz LPDDR3'-powered machine.
    Time-consuming is CPU-based training in [test_step02_pipeline.py](test_step02_pipeline.py).


## Running certain specific pipeline / test file / test cases

Example:

```
(medbert) $ python -m unittest tests/test_step01_pipelines.py
```

or running tests in some TestCase class:

```
(medbert) $ python -m unittest tests.test_pretraining_ds.PretrainingDatasetCases
```

## Running all but certain (slow) tests

TODO.

