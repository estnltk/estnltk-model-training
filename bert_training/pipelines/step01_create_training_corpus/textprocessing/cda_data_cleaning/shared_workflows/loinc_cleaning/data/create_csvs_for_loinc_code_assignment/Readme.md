# Mapping LOINC code 

**Goal**: Create csv mapping files to map `loinc_code`, `elabor_t_lyhend` and `substrate` using `analysis_name`, `parameter_name`, `parameter_unit`.


**Assumptions**: in schema  `classifications` exists a table `elabor_analysis`with columns `t_lyhend`, `t_nimetus`, `kasutatav_nimetus` and `system`.  Elabor data contains Estonian laboratory analyses information from ühendlabor. More details about the `classifications.elabor_analysis` [here](https://git.stacc.ee/project4/classifications/blob/master/loinc/elhr-digilugu-ee-algandmed/elabor_analysis.md).

**Important**: all the base mapping csv files are based on `t_lyhend` and not `loinc_code`. The reason behind it is that there were a lot of mapping files from previous pipeline, that used this kind of mapping.

Very detailed overview about mapping `t_lyhend` using `parameter_name` is [here](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/shared_workflows/loinc_cleaning/development/loinc_code_assignment/Readme_analysis_html_loinc_mapping.md).

## Step by step guide to update LOINC mapping files

### 1. Determine mapping type

Determine which kind of mapping is needed to be added. For example, does it use `analysis_name` and `parameter_name`? Then the mapping type name cotains both of those words and does not contain word unit.

Possible types:
* [analysis_parameter_name_unit_mapping_to_loinc_code](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/analysis_parameter_name_unit_mapping_to_loinc_code)
* [analysis_parameter_name_mapping_to_loinc_code](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/analysis_parameter_name_mapping_to_loinc_code)
* [analysis_name_parameter_unit_mapping_to_loinc_code](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/analysis_name_parameter_unit_mapping_to_loinc_code)
* [parameter_name_mapping_to_loinc_code](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/parameter_name_mapping_to_loinc_code)
* [parameter_name_unit_mapping_to_loinc_code](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/parameter_name_unit_mapping_to_loinc_code)


### 2. Find the right folder

Find the right folder that corresponds to the mapping type.   For example, if you want to map using `analysis_name` and `parameter_name`, open folder [analysis_parameter_name_mapping_to_loinc_code](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/analysis_parameter_name_mapping_to_loinc_code).

Important: do not changes the folders that start with `elabor_*`, those are automatic mappings.

### 3. Add information to csv file
Next, open the [csv](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/analysis_parameter_name_mapping_to_loinc_code/predefined_mapping_csvs/manual_cases.csv) file in the folder found in step 2. 

Add a new line to the end of the csv file with new information.
For example

analysis_name | parameter_name | t_lyhend
------ | ---------| -----------
Uriini analüüs 10 parameetrilise testribaga | Bilirubiin | U-Bil strip


### 4. Update the mapping files used in the workflow

Return to the `data` folder and run the file
```
./create_loinc_code_assignment.sh
```
Now the mappings are updated!

#### More information about the last step

This [create_loinc_code_assignment.sh](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_loinc_code_assignment.sh) file does two things:
1. using psql files creates an appropriate mapping table
2. copies the appropriate mapping files from different folders to the `data` folder

For example:
1.  In the folder found in step 2, there is a [psql file](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/analysis_parameter_name_mapping_to_loinc_code/create_analysis_parameter_name_to_loinc_mapping.psql) that creates a table where it adds `loinc_code` and `substrate`. This must be done using sql as there is no point doing it manually. Appropriate table:

analysis_name | parameter_name | t_lyhend | loinc_code | substrate | evidence
---- | ---- | ----- |----- |----- |----
Uriini analüüs 10-parameetrilise testribaga	| Bilirubiin	| U-Bil strip	| 41016-7	| Urine	 |manual
2. The table is copied to `data`  folder in `csv` format . Note that
`analysis_data_cleaning` workflow will use the mappings **ONLY from `data`  folder**.

The reason behind this step is 
1. All the mapping csv are located in different folders
2. Processing with SQL is needed for finding appropriate `loinc_code` and `substrate` 
3. [create_csvs_for_loinc_code_assignment](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment "create_csvs_for_loinc_code_assignment") is like a folder for development.
 This means, that all the changes done in the development are not automatically integrated to the workflow, but need to be added there using the `sh` file. 
 For example, if there is a mistake in any of the psql files, this is detected already during the copying to the data folder and not when the whole pipeline is run.

## Deprecated

## 1. elabor to `loinc_code`

### Based on parameter_name

Final result is a LOINC code mapping file: elabor_parameter_name_to_loinc_mapping.csv

### Example of result file

parameter_name | t_lyhend | t_nimetus | kasutatav_nimetus | loinc_code | evidence
--------------------|---------|------------|--------------------|------------|----
B-Hb | B-Hb | Hemoglobiin | Hb | 718-7 | t_lyhend
Hemoglobiin|B-Hb|Hemoglobiin|Hb|718-7|t_nimetus
Hb |B-Hb|Hemoglobiin|Hb|718-7|kasutatav_nimetus

### Based on parameter_name and paramter_unit
Final result is in a LOINC code mapping file: elabor_parameter_name_parameter_unit_to_loinc_mapping.csv

### Example of result file

parameter_name | t_yhik | t_lyhend | t_nimetus | kasutatav_nimetus | loinc_code | evidence
--------------------|---------|------------|--------------------|------------|----|----
Erütrotsüütide suurusjaotuvus | % | B-RDW-CV | Erütrotsüütide suurusjaotuvus | RDW-CV | 788-0 | t_nimetus + t_yhik
Erütrotsüütide suurusjaotuvus | fL|B-RDW-SD|Erütrotsüütide suurusjaotuvus|RDW-SD|21000-5|t_nimetus + t_yhik
Amikatsiin | mg/L|S,P-Amic|Amikatsiin seerumis/plasmas|Amikatsiin|35669-1|kasutatav_nimetus + t_yhik
Amikatsiin| NULL |Is-Amikacin|Amikatsiin|Amikatsiin|18860-7|kasutatav_nimetus + t_yhik



## 2. `parameter_name`  to`loinc_code`

Final result is a LOINC code mapping file: parameter_name_to_loinc_mapping.csv

Using [predefined rules from csv files](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/development/loinc_code_assignment/parameter_name_mapping_to_loinc_code/predefined_mapping_csvs) that are gotten from a previous cleaning process [here](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/analysis_data_cleaning/name_cleaning/data)

Predefined mapping .csv names:
- latest_name_cases.csv
- latest_parentheses_one_cases.csv
-   latest_parentheses_three_cases.csv
-   latest_parentheses_two_cases.csv
-   latest_prefix_cases.csv
-   latest_semimanual_cases.csv
-   latest_manual_cases.csv
- latest_completely_manual_cases.csv

All `parameter_name`s in mentioned files have characters "õ ä ö ü" replaced with "o a o u" for mapping
(in postgresql command `translate(parameter_name, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U') `)

Column `evidence` expalins from which csv file the mapping was taken.

### Example of result file

parameter_name | t_lyhend | loinc_code | evidence
--------------|---------|---------|---------------
b-MONO#(monotsuutidearv)|B-Mono#|742-7|latest_prefix_cases
B-Mono(Monotsuutide arv)|B-Mono#|742-7|latest_parentheses_one_cases
MONO% Monotsuutide suhtarv|B-Mono#|742-7|latest_manual_cases



## 3. `parameter_name` and `parameter_unit` to `loinc_code`
Final result is a LOINC code mapping file: parameter_name_parameter_unit_to_loinc_mapping.csv

Using [predefined rules](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/development/loinc_code_assignment/parameter_name_parameter_unit_mapping_to_loinc_code/predefined_mapping_csvs)
to map parameter_name and parameter_unit to loinc_code.

All `parameter_name`s in mentioned file have characters "õ ä ö ü" replaced with "o a o u".
 
 **Deleting wrong mappings**
The last step of mapping is to delete incorrect mappings. If mapping for parameter_name is created in both 
* `parameter_name` -> `loinc_code`  (steps 2 ) and 
* `parameter_name` & `parameter_unit` -> `loinc_code` (step 3)

then we delete the step 2 mapping as obviously step 3 mapping is more accurate.

 ### Example
Step 2 creates mapping

parameter_name | loinc_code
--------------|-----------
Moksifloksatsiin | 31039-1

Step 3 creates mapping

parameter_name | parameter_unit | loinc_code
--------------|--------|------
Moksifloksatsiin | NULL | 31039-1
Moksifloksatsiin | mg/L | 43751-7 

Then we delete the row from step 2 mapping as it actually depends on the unit.

### Example of result file
parameter_name | parameter_unit | t_lyhend | loinc_code | evidence
--------------|--------------|---------|---------|--------
MCH|g/L|B-MCHC|786-4|latest_subspecial_cases
B-MCH(Keskmine hemoglobiini konsentratsioon erutrotsuudis)|g/L|B-MCHC|786-4|latest_subspecial_cases
MCH|pg|B-MCH|785-6|latest_subspecial_cases
B-MCH(Keskmine hemoglobiini kogus erutrotsuudis)|pg|B-MCH|785-6|latest_subspecial_cases

