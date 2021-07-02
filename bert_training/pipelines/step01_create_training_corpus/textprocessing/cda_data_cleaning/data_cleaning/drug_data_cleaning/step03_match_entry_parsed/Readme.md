
# Matching

Input: tables `*_drug_entry_cleaned` and `drug_parsed_cleaned`.

Output: tables `*_drug_matched` and  `*_drug_matched_wo_duplicates` (without duplicates).

Output tables contain a column `match_description` which indicates based on which columns `*_drug_entry_cleaned` and `*_drug_parsed_cleaned` were matched.

File [mistakes.md](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step03_match_entry_parsed/Mistakes.md) describes known mistakes of matching.

## Input columns
In the following table are displayed the existing column names, their abbreviations (while sometimes the column names are very long) and whether the column exists in `*_drug_entry_cleaned`, `*_drug_parsed_cleaned` or both .

column name | abbreviation | exists in drug_entry or drug_parsed
-- | -- | -- |
epi_id | E |  both 
effective_time | D (like date) |  both
druc_code | DC | both
active_ingredient | ING | parsed
drug_name | DN | parsed
drug_code_display_name | DCN | entry
dose_quantity_value | DOV | both  
dose_quantity_unit | DOU  | both  
rate_quantity_value | RAV | both  
rate_quantity_unit | RAU | both  
drug_form_administration_unit_code_display_name | PT (like package type) | both



## Comments about the data

Some generalizations about the data based  on `egcut_epi`.
 
* in entry 70% of the `effective_times` are missing.
* entry `dose_quantity_value` includes sometimes also unit, so parsed `dose_quantity_value` is a subset of it
e.g. entry: "100mg", parsed: "100"
* entry `drug_code_display_name` (ingredient) is usually more detailed, but includes parsed `active_ingredient`, so the latter is subset of  `drug_code_display_name`
 e.g. entry: "Interferonum beta-1b", parsed: "Interferonum beta"
* entry package type is usually more detailed, but includes parsed package type, , so the latter is subset of  entry package type
 e.g. entry: "silmatilgad, lahus",  parsed: "silmatilgad"


## Description of matching steps    

Initially is the output  table `*_drug_matched` **empty**. After each matching step matched rows are inserted to the  table.

The following steps can produce duplicates when creating the table `*_drug_matched`. Therefore, after the matching is finished, duplicate rows will be deleted creating table  `*_drug_matched_wo_duplicates`.

There are 11 different types of matchings described on the following table. 

match_descirption| 
--|
epi_id, drug_code, active_ingredient,dose_quantity_value, dose_quantity_unit, rate_quantity_value,rate_quantity_unit, drug_form_administration_unit_code_display_name|
unique_entry|
unique_parsed|
epi_id, drug_code, active_ingredient,  dose_quantity_value, dose_quantity_unit|
epi_id, drug_code, active_ingredient, drug_form_administration_unit_code_display_name|
epi_id, active_ingredient, drug_form_administration_unit_code_display_name|
epi_id, drug_code, active_ingredient|
epi_id, active_ingredient|
epi_id, drug_code|
unresolved_entry|
unresolved_parsed|

More detailed description about each of the 10 matching steps are below.

### 1. Perfect matches E, DC, ING, DOV, DOU, RAV, RAU, PT

Matching `*_drug_entry_cleaned` and `*_drug_parsed_cleaned` based on columns
* `epi_id`
* `drug_code`
* `active_ingredient` = `drug_code_display_name`
* `dose_quantity_value`
* `dose_quantity_unit` 
* `rate_quantity_value`
* `rate_quantity_unit`
* `drug_form_administration_unit_code_display_name` 

In addition, effective_time must not be conflicting (either of the time columns is NULL or the column values are same).

Corresponding `match_description` is `epi_id, drug_code, active_ingredient,dose_quantity_value, dose_quantity_unit, rate_quantity_value,rate_quantity_unit, drug_form_administration_unit_code_display_name`


### 2. Unique epicrisis of drug_parsed/drug_entry

Epicrisis that are present in **only** one of the tables and therefore do not need to be resolved/merged.


Corresponding `match_description` is `unique_parsed` or `unique_entry`.

### 3.  Matches  E, DC, ING, DOV, DOU.
Match based on
* `epi_id`
* `drug_code`
* `active_ingredient` = `drug_code_display_name`
* `dose_quantity_value`
* `dose_quantity_unit` 

In addition, same rules apply for effective_time as in 1. 


Corresponding `match_description` is `epi_id, drug_code, active_ingredient,  dose_quantity_value, dose_quantity_unit` .

### 4. Matches  E, DC, ING, PT
Matches based on
* `epi_id`
* `drug_code`
* `active_ingredient` = `drug_code_display_name`
* `drug_form_administration_unit_code_display_name` 

Corresponding `match_description` is `epi_id, drug_code, active_ingredient, drug_form_administration_unit_code_display_name`

In addition
* same rules apply for `effective_time` as in 1.
* `dose_quantity_value`  must not be conflicting (either of the value columns is NULL or the column values are same).

### 5. Matches E, DC, ING

Matches based on
* `epi_id`
* `drug_code`
* `active_ingredient` = `drug_code_display_name`


Corresponding `match_description` is `epi_id, drug_code, active_ingredient`.

In addition, same rules apply for `effective_time` as in 1.  


### 6. Matches E, ING, PT

Matches based on
* `epi_id`
* `active_ingredient` = `drug_code_display_name`
* `drug_form_administration_unit_code_display_name` 


Corresponding `match_description` is `epi_id, active_ingredient, drug_form_administration_unit_code_display_name`.

In addition
* same rules apply for `effective_time` as in 1
* `drug_code`  must not be conflicting, either of the code columns must be NULL. The case where they are equal is covered in 4.


### 7. Matches E, ING

Matches based on
* `epi_id`
* `active_ingredient` = `drug_code_display_name`


Corresponding `match_description` is `epi_id, active_ingredient`.

In addition
* same rules apply for `effective_time` as in 1.    
* `drug_code`  must not be conflicting, either of the code columns must be NULL. The case where they are equal is covered in 4.
* `drug_form_administration_unit_code_display_name`  must not be conflicting, either of the columns must be NULL. The case where they are equal is covered in 6.

### 8. Matches E, DC
Matches based on
* `epi_id`
* `drug_code`

In addition
* same rules apply for `effective_time` as in 1.    
* `drug_form_administration_unit_code_display_name`  must not be conflicting, either of the columns must be NULL

Corresponding `match_description` is `epi_id, drug_code`.


### 9.  Unique epicrisis of drug_parsed/drug_entry

After the matching process described with 1.-7., it might be the case that we have again unique epicrisis.

For example, initially `epi_id=123` had `20` rows in `*_drug_entry` and ` 1` row in `*_drug_parsed`.  Therefore, we could only match `1` row from both tables. So `19` rows of `*_drug_entry` become unique as they **can not** and **need not** to be matched.


Corresponding `match_description` are `unique_parsed` and `unique_entry`.


### 10. Unresolved

Rows that after all the steps remain unresolved are added to the `*_drug_matced` table.

Corresponding `match_description` are `unresolved_parsed` and `unresolved_entry`.

### Delete duplicate rows
Lastly, all the duplciated rows are deleted producing table `*_drug_matched_wo_duplicates`

