# Running measurement extractions master workflow

In first terminal window start luigi daemon
```
luigid
```

In the second terminal window execute (define own prefix and configuration file)
```
export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH

prefix="run_201909261208"
conf="configurations/egcut_epi_microrun.example.ini"

luigi --scheduler-port 8082 \
--module cda_data_cleaning.fact_extraction.master_workflow RunMasterWorkflow \
--prefix=$prefix \
--config-file=$conf \
--workers=8
```

This activates the workflow of the next 4 steps:

#
1. `CreateTextsCollection2`
1. `RunAnalysisTextExtraction`
1. `CreateEventsCollection`
1. `RunExtractMeasurementsToTable`
#

Resulting in a table `<prefix>_extracted_measurements`