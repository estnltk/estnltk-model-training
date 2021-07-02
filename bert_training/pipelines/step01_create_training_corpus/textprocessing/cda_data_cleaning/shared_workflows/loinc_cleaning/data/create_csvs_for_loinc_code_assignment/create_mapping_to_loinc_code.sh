#!/bin/bash

host="10.6.6.29" #"p12.stacc.ee"
dbname="egcut_epi"
role="egcut_epi_work_create"
schema="work"

psql --host="$host" --dbname="$dbname" --file=create_csvs_for_loinc_code_assignment/analysis_name_parameter_unit_mapping_to_loinc_code/create_analysis_name_parameter_unit_to_loinc_mapping.psql -v role="$role" -v schema=$schema 2>&1 | tee


psql --host="$host" --dbname="$dbname" --file=create_csvs_for_loinc_code_assignment/elabor_parameter_name_mapping_to_loinc_code/create_elabor_parameter_name_to_loinc_mapping.psql -v role="$role" -v schema=$schema 2>&1 | tee
psql --host="$host" --dbname="$dbname" --file=create_csvs_for_loinc_code_assignment/elabor_parameter_name_unit_mapping_to_loinc_code/create_elabor_parameter_name_unit_to_loinc_mapping.psql -v role="$role" -v schema=$schema 2>&1 | tee

psql --host="$host" --dbname="$dbname" --file=create_csvs_for_loinc_code_assignment/parameter_name_mapping_to_loinc_code/create_predefined_csvs_parameter_name_to_loinc_mapping.psql -v role="$role" -v schema=$schema 2>&1 | tee
psql --host="$host" --dbname="$dbname" --file=create_csvs_for_loinc_code_assignment/parameter_name_unit_mapping_to_loinc_code/create_predefined_csv_parameter_name_unit_to_loinc_mapping.psql -v role="$role" -v schema=$schema 2>&1 | tee

psql --host="$host" --dbname="$dbname" --file=create_csvs_for_loinc_code_assignment/elabor_analysis_name_mapping_to_loinc_code/create_elabor_analysis_name_to_loinc_mapping.psql -v role="$role" -v schema=$schema 2>&1 | tee

psql --host="$host" --dbname="$dbname" --file=create_csvs_for_loinc_code_assignment/analysis_parameter_name_mapping_to_loinc_code/create_analysis_parameter_name_to_loinc_mapping.psql -v role="$role" -v schema=$schema 2>&1 | tee
psql --host="$host" --dbname="$dbname" --file=create_csvs_for_loinc_code_assignment/analysis_parameter_name_unit_mapping_to_loinc_code/create_analysis_parameter_name_unit_to_loinc_mapping.psql -v role="$role" -v schema=$schema 2>&1 | tee

