# Analysis printouts
## Extract analysis printout tables from text field

The aim is to extract analysis tables from `text` field in tables specified in  [texts_extraction_conf.py](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/texts_extraction_conf.py).

Some analysis tables are wrongly placed under other tables (procedures, anamnesis etc)
text field. This step extracts those tables from text and structures them.

Creates tables
* `*_analysis_texts`
* `*_analysis_texts_structured` 
* `*_analysis_texts_structured_cleaned`

And file
`*_quality_overview_analysis_texts_structured_cleaned`

Note: right now not a part of the pipeline because it is significantly slower then all the other parts. More information about running the following steps under [issue 1456](https://redmine.stacc.ee/redmine/issues/1456).


In first terminal window start luigi daemon
```
luigid
```

In the second terminal window execute (define own prefix and configuration file)
```
export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH

prefix="run_202101121605"
conf="configurations/egcut_epi_microrun.example.ini"

luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.event_extraction.analysis_printout_extraction.run_analysis_text_extraction RunAnalysisTextExtraction --prefix=$prefix --config=$conf --workers=1 --log-level=INFO
```

## 1. Create analysis printout tables

Creates empty tables 
1) `<prefix>_analysis_texts`
2) `<prefix>_analysis_texts_structured`

## 2. Extract analysis printout tables

**Input:** columns specified in [texts_extraction_conf.py](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/texts_extraction_conf.py)

**Output:** `<prefix>_analysis_texts`

[Extract analysis printout](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/extract_analysis_printout.py) goes through all text fields and extracts analysis table texts from them. 

There are in total 7 different types of texts, examples can be found under [example_texts](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/example_texts).

Resulting table `<prefix>_analysis_texts` has columns

column name | description| example value 
--|--|--
epi_id | epicrises id | 39859562
table_text | analysis table extracted from text field | ANALÜÜSIDE TELLIMUS nr: 525908               MATERJAL:   IP0013948355 vB 27.02.2014 14:36 (võetud: 27.02.2014 00:00)    VASTUSED: Mycoplasma pneumoniae vastane IgA seerumis Negatiivne (neg<0,8 )  Mycoplasma pneumoniae vastane IgG seerumis Negatiivne (neg<16 )   Mycoplasma pneumoniae vastane IgM seerumis Negatiivne (neg<0.8)
source_schema | schema where the text is from | original
source_table | table where the text is from | anamnesis
source_column | column where the text is from | anamsum
source_table_id | table id where the text is from | 35
text_type | which tagger type was used to extract this text|type7
span_start | span where the analysis table text started in the whole text field | 30
span_end | span where the analysis table text ended in the whole text field | 142


## 3. Extract  analysis  structured printout

**Input:** 
`<prefix>_analysis_texts`

**Output:** 
`<prefix>_analysis_texts_structured`

[Extract analysis text structured](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/extract_analysis_structured_printout.py) takes as input texts created in previous step and structures them. For each 7 types a different parser is used [analysis_text_parser](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/analysis_text_parser).

**Note:** only type5 texts are not structured because they are so chaotic that only way to give them any structure would be to use estnltk grammar.

Table `<prefix>_analysis_texts_structured` has columns

column name | description| example value 
--|--|--
epi_id| epicrises id |39859562
analysis_name_raw| uncleaned name of the analysis | Hemogramm
parameter_name_raw| uncleaned name of the parameter | WBC
value_raw | uncleaned value | 5
parameter_unit_raw| uncleaned parameter unit | E9/L) 
effective_time_raw| uncleaned effective time |26.02.2015
reference_values_raw| uncleaned reference values | 6-7 Hz sagedus
text_raw |  table row where the `*_raw` values came from | WBC 35,37x109/l, RBC 5,36x1012/l, Hgb 150g/l, Plt 726x109/l 
text_type | which tagger type was used to extract this text|type7
source_schema | schema where the text is from | original
source_table | table where the text is from | anamnesis
source_column | column where the text is from | anamsum
source_table_id | table id where the text is from | 35



## 4. Clean analysis printout
**Input:** 
`<prefix>_analysis_texts_structured`

**Output:** 
`<prefix>_analysis_texts_structured_cleaned`

**Requires:**
existence of table 
`<prefix>_possible_units`.

[Clean analysis printout](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/clean_analysis_printout.py) cleans the structured analysis texts table. It uses cleaning functions specified in [step01_create_cleaning_functions](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/analysis_data_cleaning/step00_create_cleaning_functions). 
Those functions should be improved, because analysis texts have somewhat different ways they should be cleaned.

## 5. Quality overview
**Input:** 
`<prefix>_analysis_texts_structured`

**Output:** 
`<prefix>_quality_overview_analysis_texts_structured_cleaned.csv`

[Quality overview of results](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/quality_overview_of_results.py) 
gives an overview of the column values in the final table.


## 6. Create printout layer
**Input:** 
`<prefix>_texts`
`<prefix>_texts__structure`

**Output:** 
`<prefix>_texts__printout_segments__layer`
`<prefix>_texts__printout_type1__layer`
`<prefix>_texts__printout_type2__layer`
`<prefix>_texts__printout_type3__layer`
`<prefix>_texts__printout_type4__layer`
`<prefix>_texts__printout_type5__layer`
`<prefix>_texts__printout_type6__layer`
`<prefix>_texts__printout_type7__layer`

**Requires:**
This step does not depend on steps 1.-4.!

[Create printout layer](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/event_extraction/step01_analysis_printout_extraction/create_printout_layer.py) creates a `printout_segments` layer to `texts` collection. Meaning that all printout tables are tagged in the texts.

