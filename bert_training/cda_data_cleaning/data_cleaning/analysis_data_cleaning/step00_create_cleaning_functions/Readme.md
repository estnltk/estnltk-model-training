# Creating cleaning functions

**TODO**: Problem report in `parameter_name` cleaning [issue 1464 note 16](https://redmine.stacc.ee/redmine/issues/1466?issue_count=337&issue_position=22&next_issue_id=1464&prev_issue_id=1468#note-16).


**Purpose**:  create sql functions to clean columns
- `analysis_name_raw`
- `parameter_name_raw`
- `effective_time_raw`
- `reference_values_raw`
- `value_raw`

```
luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.step00_create_cleaning_functions.create_cleaning_functions CreateCleaningFunctions --prefix=$prefix --config=$conf --workers=1 --log-level=INFO
```

Cleaning `value_raw` is the most complex part and is described [here](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/analysis_data_cleaning/step00_create_cleaning_functions/psql_cleaning_functions/value_cleaning_functions) in more detail.

**Note**: `source` is needed for creating table `possible_units` (which is used for value cleaning). 

