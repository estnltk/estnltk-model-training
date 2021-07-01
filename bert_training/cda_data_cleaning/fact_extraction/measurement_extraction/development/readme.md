# Measurement extraction development

Notes about development of measurement extraction taggers.

[Extraction rules](legacy_model/extraction_rules.txt) and [test texts](test_texts) are copied from 
[old extractor](https://git.stacc.ee/project4/extractor) (commit 5deefc267c6b23a67793eaaf9611911f806363be).

The extraction rules are used by `LegacyRegexModelTagger`.



Initially [taggers](taggers) are copied from [measurement_extraction/taggers](../taggers).

# Development cycle

1. Change the code here.
2. Run the tests.
3. Run the development workflow to see the changes in the outcome.
4. If the outcome is up to the expectation
    1. copy the development code to the [measurement extraction main folder](..),
    2. run the tests,
    3. run the measurement extraction workflow,
    4. report the achievements.
5. Go to 1. 