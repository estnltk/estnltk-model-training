#!/bin/bash

/bin/bash create_csvs_for_loinc_code_assignment/create_mapping_to_loinc_code.sh


cp ../data/create_csvs_for_loinc_code_assignment/results/elabor_parameter_name_to_loinc_mapping.csv ../data/elabor_parameter_name_to_loinc_mapping.csv

cp ../data/create_csvs_for_loinc_code_assignment/results/elabor_parameter_name_unit_to_loinc_mapping.csv ../data/elabor_parameter_name_unit_to_loinc_mapping.csv

cp ../data/create_csvs_for_loinc_code_assignment/results/parameter_name_to_loinc_mapping.csv ../data/parameter_name_to_loinc_mapping.csv

cp ../data/create_csvs_for_loinc_code_assignment/results/parameter_name_unit_to_loinc_mapping.csv ../data/parameter_name_unit_to_loinc_mapping.csv

cp ../data/create_csvs_for_loinc_code_assignment/results/elabor_analysis_name_to_panel_loinc.csv ../data/elabor_analysis_name_to_panel_loinc.csv

cp ../data/create_csvs_for_loinc_code_assignment/results/analysis_parameter_name_to_loinc_mapping.csv ../data/analysis_parameter_name_to_loinc_mapping.csv

cp ../data/create_csvs_for_loinc_code_assignment/results/analysis_parameter_name_unit_to_loinc_mapping.csv ../data/analysis_parameter_name_unit_to_loinc_mapping.csv

cp ../data/create_csvs_for_loinc_code_assignment/results/analysis_name_parameter_unit_to_loinc_mapping.csv ../data/analysis_name_parameter_unit_to_loinc_mapping.csv

psql --host=10.6.6.29 --dbname=egcut_epi --file=create_loinc_code_assignment.psql -v schema=work -v role=egcut_epi_work_create 2>&1 | tee 


