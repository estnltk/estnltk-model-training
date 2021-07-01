
# Analysis Entry

## Create analysis entry table

Luigi command
```
luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.step01_create_and_clean_analysis_entry_table.create_analysis_entry CreateAnalysisEntryTable --prefix=run_201909161129 --config=egcut_epi_microrun.example.ini --workers=1 --log-level=INFO
```



Creates table `prefix_analysis_entry` from `original.analysis_entry` which contains structured data about analyses. 

Example of created table

id|original_analysis_entry_id|epi_id|epi_type|analysis_id|code_system|code_system_name|analysis_code_raw|analysis_name_raw|parameter_code_raw|parameter_name_raw|parameter_unit_raw|reference_values_raw|effective_time_raw|value_raw|
--|--|--|--|--|--|--|--|--|--|--|--|--|--|--
142|141|20170680|a|3|1.3.6.1.4.1.28284.1.1.2.16|Analüüsid|71896|Kliinilise keemia uuringud|122|S-ALAT(Alaniini aminotransferaas)|U/l|< 55|20131220|185,7


## Clean analysis entry


Luigi command
```
luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.step01_create_and_clean_analysis_entry_table.clean_analysis_entry CleanAnalysisEntryTable --prefix=run_201909161129 --config=egcut_epi_microrun.example.ini --source=_analysis_entry --target=_analysis_entry_cleaned --workers=1 --log-level=INFO
```

1. Rearrenges values to right column because in some cases `parameter_name_raw` is under `analysis_name_raw`/`analysis_code_raw`.

2. Cleans columns:
* `analysis_name_raw`
* `parameter_name_raw`
* `effective_time_raw`
* `reference_values_raw`
* `value_raw`

New columns of the table:
* `analysis_name`
* `parameter_name`
* `effective_time`
* `reference_values`
* `value`
* `value_type`
* `suffix`
* `parameter_unit`

Example of cleaned table

id|original_analysis_entry_id|epi_id|epi_type|analysis_id|code_system|code_system_name|analysis_code_raw|analysis_name_raw|parameter_code_raw|parameter_name_raw|parameter_unit_raw|reference_values_raw|effective_time_raw|value_raw|value_type|analysis_name|parameter_name|effective_time|value| parameter_unit|suffix|value_type|reference_values
|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--
142|141|20170680|a|3|1.3.6.1.4.1.28284.1.1.2.16|Analüüsid|71896|Kliinilise keemia uuringud|122|S-ALAT(Alaniini aminotransferaas)|U/l|< 55|20131220|185,7|float|Kliinilise keemia uuringud|S-ALAT|2013-12-20 00:00:00.000000|185.7|U/l| |float|(-inf;55)


### Further explanation about cleaning `value_raw`

Cleaning `value_raw` is the most complex column to clean. Sometimes  `parameter_unit_raw` has been placed under this column.

How it is done?
1. `value_raw` is split into `prefix` and `suffix`.
	*  `prefix` is the actual value, must follow float/integer/range/ratio/number and parenthesis/time series/ text and value/ text pattern
	*  `suffix` is potentially  `parameter_unit`
2. `value_type` is assignes based on either `value_raw` or `prefix`
3.  if `suffix` is `parameter_unit` then move it under the right column
4. `value_raw`  is cleaned based on `value_type` 

Example 

Part of inital table:
value_raw | parameter_unit_raw
--|--
5,93 g/L | NULL
2+lima | NULL


1. Splitting prefix and suffix

 prefix   | suffix
 --|--
5,93 | g/L
2 | +lima

 
2.  Assigning `value_type`

For 5,93 g/L `value_type` is assigned by using `prefix`
For 2+lima `value_type` is assigned by using `value_raw`  (because `suffix` is not unit)
 
value_type |
 --|
float  |
NULL |

3.  Moving  `suffix` under  `parameter_unit`  if neccessary

value_raw | prefix | suffix| parameter_unit
 --|--|--|-- 
5,93  g/L | 5,93 | g/L | g/L
2+lima | 2 | +lima | NULL

4.  Cleaning  `value_raw`

value_raw | value | parameter_unit_raw | parameter_unit
 --|--|--|--
5,93  g/L |  5.93 | NULL | g/L
2+lima | 2+lima | NULL | NULL




