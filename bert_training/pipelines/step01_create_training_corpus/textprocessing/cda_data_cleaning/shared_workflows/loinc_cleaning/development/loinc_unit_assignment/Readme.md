# Parameter unit to  LOINC unit mapping

**Purpose**:  map `parameter_unit` to `loinc_unit`.

**How**: run command 

		 ./create_loinc_unit_mapping.sh

  which creates a `parameter_unit` to `loinc_unit` (in ühendlabor called `t_yhik`) mapping to folder [results](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/shared_workflows/loinc_cleaning/development/loinc_unit_assignment/results).

## Assumptions
* in `classification` schema exists  LOINC table from ühendlabor called `elabor_analysis`  (further information about it [here](https://git.stacc.ee/project4/classifications/tree/master/loinc/elhr-digilugu-ee-algandmed))
* exists a user provided file of all the `parameter_unit`s that want to be mapped to `loinc_unit`
--  file should be under folder `data` and called `possible_units.csv` 
-- file should look like that:


|  |
|--|
|10^12/L
|10*12/L 
|10\12/L |


## Cleaning 
Cleaning of `parameter_unit`  consists of 
* replacing `24h` with `d`
* replacing blanckspace with no space
* using file `data/unit_cleaning.csv` which contains predefined cleaning rules gotten from [here](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/analysis_data_cleaning/value_cleaning/data) and manually added rules

Example of  `data/unit_cleaning.csv`:

| dirty_unit |clean_unit|
|--|--|
|10 ^12/ L|10E12/L|
|10 12 /l|10E12/L|

## Mapping 
Mapping is based on 
* `elabor.analysis` table
* predefined mapping file `data/unit_to_loinc_unit_mapping.csv`

Example of  `data/unit_to_loinc_unit_mapping.csv`:

| parameter_unit | loinc_unit
| --|--
| 10E12/L| e12/l
| 10\12/L | e12/l

## Results
Results in 
1. a mapping file `results/parameter_unit_to_loinc_mapping.csv`
Where to every `parameter_unit` a `loinc_unit` is mapped. Column `evidence` explains, if the mapping was gotten from elabor or predefined rules.
Example of results file:

parameter_unit| clean_unit | loinc_unit|evidence
--|--| -- | --
10E12/l | 10E12/L | E12/L | manual_mapping	
x10^12/l | 10E12/L| E12/L | manual_mapping
µg/mL | ug/mL | MG/L | manual_mapping
mm Hg | mmHg |mmHg|elabor_tyhik
mM| mmHg | mmHg|elabor_tyhik
u/L|U/L| U/L|elabor_tyhik
U/l|U/L|U/L|elabor_tyhik
u/l|U/L|U/L|elabor_tyhik

2. a cleaning file `results/parameter_unit_to_cleaned_unit.csv` where are all the `parameter_unit`s that we didn't know how to map to `loinc_unit` with their cleaned values.
Example of file:

parameter_unit| clean_unit | loinc_unit|evidence
--|--| -- | --
10*3 µl | 10E3/uL | 
*10³/µl |  10E3/uL
g/dL | g/dL |
µg/dl | ug/dL
