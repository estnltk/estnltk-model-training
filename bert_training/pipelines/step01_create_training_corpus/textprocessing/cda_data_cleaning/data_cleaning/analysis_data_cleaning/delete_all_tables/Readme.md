# Delete tables

This luigis workflow deletes all the tables created during running the `analysis_data_cleaning` pipeline with a given prefix.

The whole process can be executed with the following commands in terminal.
In first terminal window start luigi daemon
```
luigid
```

In the second terminal window execute (choose own prefix and configuration file)
```
export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
prefix="run_202101181300_"
conf="egcut_epi_microrun.example.ini"
luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.analysis_data_cleaning.delete_all_tables.delete_all_tables DeleteAllTables --prefix=$prefix --config=$conf --workers=1 --log-level=INFO
```
where tables with the give prefix are deleted.

The deleted table names are:
* 'analysis_cleaned',
* 'analysis_entry',
* 'analysis_entry_cleaned',
* 'analysis_entry_loinced', 
* 'analysis_entry_loinced_unique', 
* 'analysis_html', 
* 'analysis_html_cleaned', 
* 'analysis_html_log', 
* 'analysis_html_loinced', 
* 'analysis_html_loinced_unique', 
* 'analysis_html_meta', 
* 'elabor_parameter_name_parameter_unit_to_loinc_',
* 'elabor_parameter_name_to_loinc_mapping', 
* 'long_loinc_mapping', 'matched', 
* 'parameter_name_parameter_unit_to_loinc_mapping',
* 'parameter_name_to_loinc_mapping',
* 'parameter_unit_to_cleaned_unit',
* 'parameter_unit_to_loinc_unit_mapping', 
* 'tie'.


