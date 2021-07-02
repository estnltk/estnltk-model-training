# LOINC code mapping csvs

Creates LOINC code mapping csv files to `data` folder. It copies the files from folder `data/create_csvs_for_loinc_code_assignment/results` to  `data/`.
Those files from  `data` folder will be imported to database during luigi task in `create_loinc_mappings.py`.

If some improvements are wanted to be made in LOINC mapping then 
1. change the psql scripts in folder `data/create_csvs_for_loinc_code_assignment` or csv files in the folder `data/create_csvs_for_loinc_code_assignment`
2. run the sh file in current folder `data/create_loinc_code_assignment.sh`
    
    2.1 The sh file first runs `data/create_csvs_for_loinc_code_assignment/create_mapping_to_loinc_code.sh` which creates new updated tables to results (`data/create_csvs_for_loinc_code_assignment/results`) folder
    
    2.2  Then, the csv tables are copied from results folder to `data` folder

What is important to note, is that the csv mappings (`data/create_csvs_for_loinc_code_assignment`) are based on `t_lyhend` and not `loinc_code`. Therefore, new mappings should be added by  `t_lyhend`. For example, mapping table looks like that

analysis_name | parameter_name | t_lyhend
-- | -- | --
24356-8 Uriini Sademe Mikroskoopia	| 1756-6 Liikvori- ja seerumialbumiini suhe	| CSF-Alb/S-Alb

Loinc code and substrate are added later using psql code.

