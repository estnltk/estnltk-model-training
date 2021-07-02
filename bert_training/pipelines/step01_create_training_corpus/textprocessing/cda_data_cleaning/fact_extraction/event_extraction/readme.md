# Event extraction 

The event extraction workflows typically assume that there is an input database:
* `original` -- the entire set of epicrises
* `original_microset_fixed` -- small set of epicrises meant for debugging

There is an another way to select data for the analysis. In that case you have to assemble
the events manually to a source table and run a specific import task (???).
This way of importing data is mostly used in developement where specific text collections
are needed to refine fact extraction pipelines.

In both cases, the workflow creates two collections:
* `*_texts`
* `*_events`


## Configuration
The configuration is read from the configuration file in the cda-data-cleaning/configurations folder.
* The name of the configuration file is the `--conf` parameter for `luigi`.

See the example configuration files 
* `egcut_epi.example.ini` 
* `egcut_epi_microrun.example.ini`
* ...

to create your own files
* `egcut_epi.ini`
* `egcut_epi_microrun.ini`

## Run workflows 
To run the following workflows `path-to/cda-data-cleaning` must be in the `PYTHONPATH` (e.g. `export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH`).

1. First analysis printout extraction workflow must be run. 
This creates `printout_segments` layer to `texts` collection.
```
luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.event_extraction.step01_analysis_printout_extraction.run_analysis_text_extraction RunAnalysisTextExtraction --prefix=$prefix --config=$conf --workers=1 --log-level=INFO
```

2.  Secondly, event extraction should be ran. This creates layers 
* event_tokens
* event_tokens2
* event_segments
* event_headers
to `texts` collection (in the given order)  and also creates event collection.
```
luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.event_extraction.step02_create_events_collection.create_events_collection CreateEventsCollection --prefix=$prefix --conf=$conf --workers=8
```

