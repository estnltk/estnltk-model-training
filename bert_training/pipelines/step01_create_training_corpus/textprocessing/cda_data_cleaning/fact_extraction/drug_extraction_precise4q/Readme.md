
# Drug extraction from free text fields in epicrisis documents

Input:  table `original.anamnesis` and `original.sumsum`  

Ouput:  `*_drug_free_text`  (where '*' denotes the prefix)


The whole process can be executed with the following commands in terminal.
In first terminal window start luigi daemon
```
luigid
```

In the second terminal window run the whole pipeline (choosing own prefix for tables and configuration file values)
```
export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
prefix="run_202004031544_"
conf="egcut_epi_microrun.ini"

luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.drug_extraction_precise4q.drug_extraction ExtractPrecise4qDrugEntries --prefix=$prefix --config=$conf --workers=1 --log-level=INFO
```
For output tables to be without prefix, just remove ```prefix=$prefix``` from the previous command.

## Libraries

Which versions were used during development:
* estnltk 1.6.5
* luigi 2.8.3
* pandas 0.25
* pyscopg2 2.8.4
* tqdm 4.42.1
