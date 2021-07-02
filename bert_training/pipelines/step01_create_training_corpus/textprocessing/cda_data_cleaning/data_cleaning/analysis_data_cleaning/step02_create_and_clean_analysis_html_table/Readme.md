# Create analysis HTML table

Table `*_analysis_html` for parsed analysis HTML tables is created. Columns and data types are displayed in the following table.

column name | column type | description |
--|--|--|
row_nr | int| rows are numbered inside every epicrisis
epi_id | varchar| epicrisis id
epi_type | varchar| a or s
parse_type | varchar| 7 different types of HTML tables: 1.0,2.1,2.2,2.3,2.4,3.0,4.0
panel_id | int| one epicrisis can contain multiple panels, to tell the difference between panels inside epicrisis, they are numbered with panel_id
analysis_name_raw | varchar| uncleaned analysis_name
parameter_name_raw |varchar|  uncleaned parameter_name
parameter_unit_raw | varchar| uncleaned parameter_unit
reference_values_raw | varchar| uncleaned reference_values
effective_time_raw |varchar| uncleaned effective_time
value_raw | varchar| uncleaned value
analysis_substrate_raw |varchar| uncleaned analysis_substrate


Log table for parsing is created  `*_analysis_html_log ` with columns

column name | column type | description |
--|--|--|
time |varchar| time that the table was parsed
epi_id | varchar| epi id for the given table
 type | varchar| either `error` or `parse_type`
 message |varchar| description of  `type` <ul><li>1.0</li><li>2.1</li><li>2.2</li><li>2.3</li><li>2.4</li><li>3.0</li><li>4.0</li><li>{""Less columns in body than header labels, one column is empty""}</li><li>{"Warning: Long table has value = \"-\" which will be deleted during cleaning"}</li></ul>



Table for meta data is created    `*_analysis_html_meta`.  Stores all the type 4.0 HTML table
<ANONYM  ... > values.

  column name | column type | description |
--|--|--|
 epi_id | varchar|epicrisis id
 attribute | varchar| right now only 'Description:' more information can be added in the future
 value | varchar| stores <ANONYM  ... > values, example <ul><li>{"Hindaja: <ANONYM id=\"0\" type=\"per\" morph=\"_H_ sg n;_H_ sg n\"/> Hindaja asutus: LÕUNA-EESTI HAIGLA AS (10833853) Analüüsi aeg: 15.12.2015 16:26"}</li><li>{"Hindaja: (AV1) <ANONYM id=\"6\" type=\"per\" morph=\"_H_ sg n\"/> <ANONYM id=\"7\" type=\"per\" morph=\"_H_ sg n\"/> Hindaja asutus: AS LÄÄNE-TALLINNA KESKHAIGLA (10822269) Analüüsi aeg: 13.12.2015 19:59"}</li></ul>


# Parse HTML tables
  
 Parsing HTML tables to POSTGRESQL tables according to parse_type. More detailed overview [here](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/data_cleaning/step02_create_and_clean_analysis_html_table/parsing.md) and [here](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/data_cleaning/step02_create_and_clean_analysis_html_table/parsing_new_parse_types.md).



# Cleaning analysis HTML

Detailed information how cleaning works and the functions used for cleaning is under [step00_create_cleaning_functions](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/step00_create_cleaning_functions).

 For cleaning `source` table execute following luigi task
```
luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.step02_create_and_clean_analysis_html_table.clean_analysis_html_table CleanAnalysisHtmlTable --prefix=$prefix --config=$conf --source=_analysis_html --target=_analysis_html_cleaned --workers=1 --log-level=INFO
```

where
* `source` is the name of the table that is going to be cleaned
* `target` is the name of the table which will have cleaned columns

## Example

`source` table `*_analysis_html`

|row_nr| epi_id| epi_type| parse_type|	panel_id| analysis_name_raw |parameter_name_raw| parameter_unit_raw| reference_values_raw| effective_time_raw | value_raw|analysis_substrate_raw|id
|--|--|--|--|--|--|--|--|--|--|--|--|--|
719|35359280|a|2.4|18|a2033 Hemogramm|a2085 Valk ||NEG|06.09.2011|5.00 g/L| |3031
11|20170680|a|2.3|2|Kliinilise keemia uuringud|S-CRP(C-reaktiivne valk)|mg/l|<5,0|20.12.2013|< 0.2| |173
23|39808887|s|1.0|8|Happe-aluse tasakaal arteriaalses veres|%sO2 c|%|95-99|2015.03.21|98,9(07:26),94,6(12:34),97,5(17:00)||1211



`target` table `*_analysis_html_cleaned`

|row_nr| epi_id| epi_type| parse_type|	panel_id| analysis_name_raw |parameter_name_raw| parameter_unit_raw| reference_values_raw| effective_time_raw | value_raw|analysis_substrate_raw|id | cleaned_value | analysis_name|parameter_name|effective_time|value| parameter_unit_from_suffix|suffix|value_type|reference_values|
|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
719|35359280|a|2.4|18|a2033 Hemogramm|a2085 Valk| |NEG|06.09.2011|5.00 g/L||3031|"{5.0,g/L,g/L,float}"|Hemogramm|Valk|2011-09-06 00:00:00.000000|5.0|g/L|g/L|float|NEG
11|20170680|a|2.3|2|Kliinilise keemia uuringud|S-CRP(C-reaktiivne valk)|mg/l|<5,0|20.12.2013|< 0.2| |173|"{(-Inf;0.2),mg/l,,range}"|Kliinilise keemia uuringud|S-CRP|2013-12-20 00:00:00.000000|(-Inf;0.2)|mg/l| |range |(-Inf;5.0)
23|39808887|s|1.0|8|Happe-aluse tasakaal arteriaalses veres| %sO2 c|%|95-99|2015.03.21|98,9(07:26),94,6(12:34),97,5(17:00)| |1211|{98,9(07:26),94,6(12:34),97,5(17:00),,%,(07:26),94,6(12:34),97,5(17:00),,time_series} | Happe-aluse tasakaal arteriaalses veres| sO2 c% |2015-03-21 00:00:00.000000| 98,9(07:26),94,6(12:34),97,5(17:00) |%|(07:26),94,6(12:34),97,5(17:00)| time_series| [95,99]|









