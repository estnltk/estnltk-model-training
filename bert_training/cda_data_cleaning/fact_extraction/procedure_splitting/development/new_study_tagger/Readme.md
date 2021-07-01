# Study header tagger

The whole process can be executed with the following commands in terminal.
In first terminal window start luigi daemon
```
luigid
```

In the second terminal window run the whole pipeline (example of prefix for tables and configuration file and pythonpath given below)
```
export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
prefix="run_202006251717_"
conf="egcut_epi_microrun.ini"
luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.procedure_splitting.development.new_study_tagger.find_study_header FindStudyHeader --prefix=$prefix --config=$conf --sourceschema="original" --sourcetable="procedures" --targetschema="work" --targettable="study_headers" --workers=1 --log-level=INFO
```
Note that source table must contain columns  `text`, `epi_id`, `epi_type`, `id`. 