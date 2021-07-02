set role egcut_epi_analysiscleaning_create;

drop table if exists analysiscleaning.loinc_analytes;
create table analysiscleaning.loinc_analytes
(
	analyte	text,
	analyte_name_en text,
	analyte_name_ee text
);
/*command in psql
psql --host=p12.stacc.ee --dbname=egcut_epi
\copy analysiscleaning.loinc_analytes from 'Repos/cda-data-cleaning/analysis_data_cleaning/LOINC_cleaning/full_names/loinc_analytes.csv' with (delimiter ',', format csv, header);
*/

drop table if exists analysiscleaning.loinc_properties;
create table analysiscleaning.loinc_properties
(
	property	text,
	property_name_en text,
	property_name_ee text
);
/*\copy analysiscleaning.loinc_properties from 'Repos/cda-data-cleaning/analysis_data_cleaning/LOINC_cleaning/full_names/loinc_properties.csv' with (delimiter ',', format csv, header);*/

drop table if exists analysiscleaning.loinc_systems;
create table analysiscleaning.loinc_systems
(
	system	text,
	system_name_en text,
	system_name_ee text
);