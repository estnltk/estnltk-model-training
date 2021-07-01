# Match entry and HTML tables.

Goal is to match together (cleaned and loinced) entry table and  (cleaned and loinced) HTML table. The result is stored in table `*_analysis_matched`.

Matching is based on different column combinations of 
`epi_id`, `analysis_name` `parameter_name` `parameter_unit` `effective_time` `value` `reference_values`.  
In the matched table column `match_description` describes on which combination HTML and entry were matched.

First it tries to match based on all the columns mentioned above. If it does not succeed, then it tries with less columns and so on. Column `epi_id` must always match.

## Example of created table
Example is taken from epicrisis 10794563. All the columns (`epi_id`, `analysis_name` `parameter_name` `parameter_unit` `effective_time` `value` `reference_values`)  are identical and therefore the rows are matched.

Note: `original_analysis_entry_id` corresponds to `original.analysis_entry` column `original_analysis_entry_id`.

column name | value
--|--
id_html|22277
id_entry|17623
row_nr_html|99
epi_id_html|10794563
parse_type_html|1.0
panel_id_html|227
analysis_name_raw_html|Glükoos seerumis
parameter_name_raw_html|S-Gluc
parameter_unit_raw_html|mmol/L
reference_values_raw_html|4.2 - 6.4
value_raw_html|4.8
analysis_substrate_raw_html| NULL
effective_time_raw_html|18.11.2011
analysis_name_html|Glükoos seerumis
parameter_name_html|S-Gluc
effective_time_html|2011-11-18 00:00:00.000000
value_html|4.8
parameter_unit_from_suffix_html|mmol/L
suffix_html|
value_type_html|float
time_series_block_id_html | 1
reference_values_html|[4.2,6.4]
parameter_unit_html|mmol/L
loinc_unit_html|mmol/L
substrate_html|Ser/Plas
elabor_t_lyhend_html|S,P-Gluc
loinc_code_html|14749-6
source_html|html
original_analysis_entry_id|17804
epi_id_entry|10794563
analysis_id_entry|200
code_system_entry|1.3.6.1.4.1.28284.1.170.2.16
code_system_name_entry|ESTER analüüs
analysis_code_raw_entry|NULL
analysis_name_raw_entry |Glükoos seerumis
parameter_code_raw_entry|S-Gluc
parameter_name_raw_entry|S-Gluc
parameter_unit_raw_entry|mmol/L
reference_values_raw_entry|4.2 - 6.4
effective_time_raw_entry|20111118
value_raw_entry|4.8
analysis_name_entry|Glükoos seerumis
parameter_name_entry|S-Gluc
effective_time_entry|2011-11-18 00:00:00.000000
value_entry|4.8
parameter_unit_from_suffix_entry|mmol/L
suffix_entry| NULL
value_type_entry| float
time_series_block_id_entry | 2
reference_values_entry|[4.2,6.4]
parameter_unit_entry|mmol/L
loinc_unit_entry|mmol/L
substrate_entry|Ser/Plas
elabor_t_lyhend_entry|S,P-Gluc
loinc_code_entry|14749-6
source_entry|entry
match_description|analysis_name, parameter_name, parameter_unit, effective_time, value, reference_values


More details about matching [here](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/data_cleaning/step05_match_entry_and_html/Readme_matching.md).


# Finaly table where entry and HTML are joined

Creates final table named  `*_analysis_cleaned`. 
If `*_entry` and `*_html` have different values, then we prefer HTML table, though it **might not always be right**. 


## Final table

| Column name | Meaning | Example |
|:-------|:---|:---|
| epi\_id                  | epicrisis ID                          | ***                                  |
| loinc_code               | LOINC code                            | 704-7                                |
| elabor_t_lyhend          | Standard Estonian abbreviation        | B-Baso#                              |
| analysis_name            | name of the medical test              | Hematoloogilised ja uriini uuringud  |
| parameter_name           | name of the analyte                   | BASO                                 |
| parameter_unit           | measurement unit                      | 10L                                  |
| effective_time           | measurement time                      | 2012-03-09 00:00:00                  |
| value                    | measurement value                     | 0.01                                 |
| reference_values         | range of normal measurement values    | [0,0.1]                              |
| id_entry                 | row id in analysis_entry, analysis_entry_cleaned, analysis_entry_loinced, analysis_entry_loinced_unique  | 571525	                              |
| id_html                  | row id in analysis_html_cleaned, analysis_html_loinced, analysis_html_loinced_unique          | 811854                               |
|time_series_block_id      | indicates that the value belonged initially to time series (if NULL, then did not). If rows for the same epi_id share time_series_block_id, then they came from the same time_series | 3
| match_description        | how sources where matched             | epi_id, parameter_name, ..., value   |
| ---                      |   --- original uncleaned fields ---   | ---                                  |
| analysis_substrate_raw   | analysis substrate in the html table  | täisveri                             |
| code_system_entry        | code of the measurement coding system | 1.3.6.1.4.1.28284.1.28.2.16          |
| code_system_name_entry   | name of the measurement coding system | Analüüsid                            |
| analysis_code_raw_entry  | code of the medical test              | 11122                                |
| parameter_code_raw_entry | code of the measured analyte          | 11116                                |
| analysis_id_entry        | id row of the original entry table    | 798                                  |
| analysis_name_raw        | name of the medical test              | Hematoloogilised ja uriini uuringud  |
| parameter_name_raw       | name of the analyte                   | BASO(BASO#)                          |
| parameter_unit_raw       | measurement unit                      | 10 l                                 |
| effective_time_raw       | measurement time                      | 09.03.2012                           |
| value_raw                | measurement value                     | 0,01                                 |
| reference_values_raw     | range of normal measurement values    | 0 - 0,1                              |
| substrate_entry          | substrate of the row (Bld, Urine, Ser/Plas) in entry       | Bld 571525	                              |
| substrate_html           | substrate of the row (Bld, Urine, Ser/Plas) in html | Ser/Plas               |
| value_type               | type of the recorded cleaned value                  | float                                |

