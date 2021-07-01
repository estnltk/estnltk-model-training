# Diagnosis

Execution of the luigi workflow in terminal

1. Keep luigi running the whole time in terminal
``` 
luigid
 ```
2. Set the prefix of the created table 
```
prefix="run_$(date +"%Y%m%d%H%M")"
```
3. Set the [configuration](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/configurations) file to be used 
 ```
 conf="egcut_epi_microrun.example.ini"
 ```
4.  Run `diagnosis_extra.py` luigi workflow 
```
luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.diagnoses_extra.diagnoses_extra Diagnoses --prefix=$prefix --config=$conf --source=main_diagnosis --target=diagnoses_extra --workers=1 --log-level=INFO
```
Parameter `source` is for the source table that will be used in the workflow.
Parameter `target` is the table created at the end of the workflow.

All the parameters (prefix, config, source, target) are not mandatory and can be omitted. But then they must be deleted from  `diagnosis_extra.py`  as well.
