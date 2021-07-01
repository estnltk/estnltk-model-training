
# Unit conversion

## Purpose
Find the units that do not match and create a file about conflicting units.
The files can be used to improve `parameter_unit` -> `loinc_unit` mapping.

## Description 
There are two ways to map units to analysis in HTML table:
1.  `parameter_name` -> `loinc_code` -> `loinc_unit`
      Each analysis in HTML table parsing runfull201903041255_analysis_html has been mapped a `loinc_code`. Each `loinc_code` has a corresponding unit called `t_yhik`.

2. `parameter_name` -> `parameter_unit` -> `loinc_unit`
  Each analysis has `parameter_unit_raw`. After cleaning those units, they can be mapped to `loinc_unit`s as well. This is done in [here](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/analysis_data_cleaning/LOINC_cleaning/loinc_unit_assignment)

For validating our `loinc_code` mapping units from 1. and 2. are compared. Conflicting units are written into files under `results`  that should be manually checked.

## Assumptions
1. exists a table with columns
* `loinc_code`,
* `parameter_unit_raw` 
* `parameter_name_raw`
right now the table used  is analysis_html_loinced2_runfull201904041255 (contains duplicates!) so should be changed to a properly loinced html table.

2. in `classification` schema exists table `elabor_analysis`

## Result 
Files:

1. Result file `unit_conversion_parameter_name.csv` 
all the rows with conflicting units.

loinc_unit | t_yhik | parameter_name_raw | count
------------| -------| --------------------------|---- 
10*3 µl | E9/L | PLT | 665
10*3 µl | E9/L | WBC | 665
10*3 µl | E9/L | MONO# | 650
10*3 µl | E9/L | LYMPH# | 650
IU/L | U/L | S-FSH | 370
IU/L | U/L | S-hCG | 305

2. Result file `unit_conversion_parameter_name.csv`
Indicates how unit mapping file (`loinc_unit_assignment/data/unit_to_loinc_unit_mapping.csv`) could be made better.
For example it is highly likely that 10*3 µl corresponds to E9/L, for IU/L most likely is U/l and for  KIU/L most likely should be mapped to kU/L etc. Rows with % have likely WRONG LOINC codes.

loinc_unit | t_yhik  | count
------------| -------|---- 
10*3 µl | E9/L | 4655
IU/L | kU/L | 63
IU/L | mU/L | 59
IU/L | U/L | 1151
KIU/L | kU/L | 4410
KIU/L | U/L | 1
%|/100WBC|179
%|E9/L|5829
%|fL|698

