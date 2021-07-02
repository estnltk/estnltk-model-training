# Drug data cleaning

Input:  table `original.drug_entry` and `original.drug` column `text` 

Ouput:  `*_drug_cleaned`  (where '*' denotes the prefix)

Goal is to take input tables, then extract information from  `text` column, clean both tables and match rows from into one table.

The whole process can be executed with the following commands in terminal.
In first terminal window start luigi daemon
```
luigid
```

In the second terminal window run the whole pipeline (choosing own prefix for tables and configuration file values)
```
export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
prefix="run_202004031544_"
conf="configurations/egcut_epi_microrun.ini"
luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.drug_data_cleaning.run_drug_data_cleaning RunDrugDataCleaning --prefix=$prefix --config=$conf --workers=1 --log-level=INFO
```
For output tables to be without prefix, just remove ```prefix=$prefix``` from the previous command.

Now all the following four steps are executed.

## [Step01](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step01_parse_drug_text_field) 
Extracts drug information from  `text` field in  `original.drug` and stores it in postgres table  `*_drug_parsed`.

## [Step02](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step02_parse_and_clean_drug_text_field)
Cleans both `original.drug_entry` and `*_drug_parsed` tables.

## [Step03](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step03_match_entry_parsed)
Matches both cleaned tables from step02 and deletes duplicate rows.

## [Step04](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step04_final_table)
Creates final table combining information from both tables.


## Libraries 

Which versions were used during development:
* estnltk 1.6.5
* luigi 2.8.3
* pandas 0.25
* pyscopg2 2.8.4
* tqdm 4.42.1


