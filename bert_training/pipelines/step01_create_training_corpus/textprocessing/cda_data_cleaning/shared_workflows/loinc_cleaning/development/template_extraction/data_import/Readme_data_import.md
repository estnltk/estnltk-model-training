- Initial_analysis_seperation 
    - seperates blood, urine and csf
    - creates files
        * raw_data/blood.csv
        * raw_data/urine.csv
        * raw_data/csf.csv
    

- Analysis_classification 
    - creates a file where all the analysis_names are classified as either
    blood or urine raw_data/analysis_classified.csv
    - classification is based on initial_analysis_seperation

        
- Create_analysis_panel_extraction
   - Takes all the unique panels and creates table with their templates with details. 
(For every panel template we choose an epi_id to represent it.)
   - In result it keeps only panels with multiple parameter (Baso, Eo, Lymph etc) values.
   - contains columns
        * epi_id
        * analysis_name
        * parameter_code
        * parameter_name
        * unit
  - Last 4 columns are needed for mapping loinc
  
- create_blood_templates.sql and create_urine_templates
    - SAME as create_analysis_panel_extraction BUT creates a table containing only 
    blood OR urine related analysis_names (specified in analysis_classification)
    - contain templates of all blood or urine panels
    - based on the data of those tables loincing is preformed
    - needed for blood_mapping.ipynb and urine_mapping.ipynb


- create_analysis_entry_type_counts_csv 
    - creates a file raw_data/analysis_entry_type_counts.csv


