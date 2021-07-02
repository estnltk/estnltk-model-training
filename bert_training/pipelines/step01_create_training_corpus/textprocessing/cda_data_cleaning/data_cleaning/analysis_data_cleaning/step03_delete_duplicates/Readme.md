# Apply LOINC mapping and delete duplicates

## LOINC mapping
LOINC mapping with Readme is stored under [shared_workflows](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning).

## Delete duplicates

During parsing HTML/cleaning HTML/LOINC mapping duplicate rows are created.
This step deletes duplicate rows from both entry and html table.

Luigi command 
``` 
luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.step04_delete_duplicates.delete_duplicates DeleteDuplicates --prefix=$prefix --config=$conf --workers=1 --log-level=INFO
``` 

### Entry 
Considers rows in entry duplicate, if following columns are identical:
* epi_id,  
* epi_type
* code_system,          
*  code_system_name,  
 * analysis_code_raw, 
 * parameter_code_raw, 
 * analysis_name_raw, 
 * parameter_name_raw,
 * parameter_unit_raw, 
 * reference_values_raw, 
 * value_raw, 
 * effective_time_raw, 
 * cleaned_value, 
 * analysis_name, 
 * parameter_name,
 *  effective_time, 
 * value,
 *  parameter_unit, 
 * suffix, 
 * value_type,
 * reference_values



### HTML
Considers rows in HTML duplicate, if following columns are identical:
* epi_type,
* parse_type, 
* panel_id, 
* analysis_name_raw, 
* parameter_name_raw, 
* parameter_unit_raw, 
* reference_values_raw, 
* value_raw, 
* analysis_substrate_raw,
* effective_time_raw, 
* cleaned_value, 
* analysis_name, 
* parameter_name,
 * parameter_unit, 
 * effective_time, 
 * value, 
 * suffix, 
 * value_type, 
 * reference_values

From duplicated rows, only one rows is kept and the other one(s) are deleted.

