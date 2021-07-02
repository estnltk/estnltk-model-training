
# LOINC 

**Purpose**:  assign LOINC code to observations in the source table.

Right now a part of [`analysis_data_cleaning` pipeline](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/analysis_data_cleaning).

## Create LOINC mappings

Imports csv mapping files to the database.
Mapping files can be found in the [data folder](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data).

Imported LOINC code mapping table names:
*  `<prefix>_parameter_name_to_loinc_mapping`
*  `<prefix>_parameter_name_unit_to_loinc_mapping`
*  `<prefix>_elabor_parameter_name_to_loinc_mapping`
*  `<prefix>_elabor_parameter_name_unit_to_loinc_mapping`
*  `<prefix>_elabor_analysis_name_to_panel_loinc`
*  `<prefix>_analysis_parameter_name_to_loinc_mapping`
*  `<prefix>_analysis_parameter_name_unit_to_loinc_mapping`
* `<prefix>_analysis_name_parameter_unit_to_loinc_mapping`

Imported LOINC unit mapping table names:
* `<prefix>_parameter_unit_to_cleaned_unit`
* `<prefix>_parameter_unit_to_loinc_unit_mapping`

Imported substrate mapping table names:
* `<prefix>_long_loinc_mapping`
* `<prefix>_long_loinc_mapping_unique`


## Apply LOINC mappings

Input:
* `<prefix>_analysis_cleaned_html`
* `<prefix>_analysis_cleaned_entry`

Output:
* `<prefix>_analysis_html_loinced`
* `<prefix>_analysis_entry_loinced`

### 1. Assign LOINC units

Based on `parameter_unit_raw` assigns `parameter_unit` and `loinc_unit` to each row.
Creates new columns `parameter_unit` and `loinc_unit`.
 
### 2. Assign substrate

Based on `analysis_name` and `parameter_name` assigns `substrate` to source table (`bld`,`urine`,`ser/plas`,`fSP`,`cB`).  
This makes LOINC mapping more accurate as some analytes can be measured both in blood and urine.

### 3. Bring back lost information

In `parameter_name_raw` cleaning all information from parenthesis is deleted, but in few cases it is actually needed for  accurate LOINC mapping. 
 Therefore, bring back the information from parenthesis whether necessary updates column - 'parameter_name'

### 4. Create output tables

Creates target tables
* `<prefix>_analysis_html_loinced`
* `<prefix>_analysis_entry_loinced` 
with extra columns
 * `loinc_code` (empty column) 
 * `elabor_t_lyhend` (empty column)
 * `substrate`

### 5. Apply mapping to source

In this step the columns `loinc_code`  and `elabor_t_lyhend` filled in.

Mapping for each row is performed by matching imported mapping files columns with input table columns. Different combinations of columns
* `analysis_name`
* `parameter_name`
* `parameter_unit`
* `loinc_unit`
* `parameter_unit_from_suffix`
* `substrate`
are used for mapping.

### Column description
Description of columns added during loincing.

column_name | description | example
---- | ----- | ----
`loinc_code` | loinc code from classification elabor table, in rare cases mapped manually, if the code does not exist in elabor | 58805-3
`loinc_unit` | unit corresponding to the loinc code in classifications elabor table | E6/L
`elabor_tlyhend` | t_lyhend from the elabor classfications table | U-WBC strip
`parameter_unit` | cleaned `parameter_unit_raw` | /uL
`substrate` | substrate where the analysis was taken from (usually blood, urine or serum plasma) | bld

## Example

Example of **source** table:

|row_nr| epi_id| epi_type| parse_type|	panel_id| analysis_name_raw |parameter_name_raw| parameter_unit_raw| reference_values_raw| effective_time_raw | value_raw| value_type | parameter_name | value|
|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
1|281432|s|2.1|57|Hematoloogilised uuringud|WBC|10 U/l| | 2010-01-07 00:00:00.000000|11,6| float | WBC | 11.6 |
11|1281432|s|2.1 |57|Hematoloogilised uuringud|%NEUT|%| |2010-01-07 00:00:00.000000|75,3 | float | NEUT% |75.3 |

Important columns that are used for mapping are
* `parameter_name_raw`
* `parameter_unit_raw`

Example of **target** (output) table:

|row_nr| epi_id| epi_type| parse_type|	panel_id| analysis_name_raw |parameter_name_raw| parameter_unit_raw| reference_values_raw| effective_time_raw | value_raw| loinc_code | elabor_tlyhend | value_type | parameter_name | value| parameter_unit | loinc_unit |elabor_tlyhend | loinc_code|
|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
1|1281432|s|2.1|57|Hematoloogilised uuringud|WBC|10 U/l| |2010-01-07 00:00:00.000000 | 11,6 | 6690-2 | B-WBC | WBC| float  | 11.6 |   10E9/L|E9/L|B-WBC|6690-2|
11|1281432|s|2.1| 57|Hematoloogilised uuringud|%NEUT|%| |2010-01-07 00:00:00.000000|75,3 |770-8 |B-Neut% | NEUT%| float |75.3 |%|%|B-Neut%|770-8|



## Mapping data

In case the LOINC mapping files need to be improved, then it should be done in [`data/create_csvs_for_loinc_code_assignemnt` folder](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment). In that folder, you can find different subfolders for each different kind of mapping. In each subfolder there is a csv file, where the new mappings can be added to the end of that csv file.

There is a step by step guide for updating the LOINC mappings with an example [here](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/shared_workflows/loinc_cleaning/data/create_csvs_for_loinc_code_assignment/Readme.md).

