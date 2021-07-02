- blood_mapping.ipynb

    - Takes epi_id, analysis_name, parameter_name, parameter_code and unit 
    from data_import/create_blood_templates.sql as input
    - Accordingly assigns 
        * system 
        * property
        * analyte
    - Analysis that are blood related 
    
- urine_mapping.ipynb
    - Takes epi_id, analysis_name, parameter_name, parameter_code and unit 
    from data_import/create_urine_templates.sql as input
    - Accordingly assigns 
        * system_type
        * property 
        * analyte
    - Analysis that are urine related
    
- cleaning_functions.py and data_import.py
    - needed for running blood_mapping.ipynb and urine_mapping.ipynb