/*Lisame KÕIGILE mappingu ridadele sama time aspecit ja scale'i*/
ALTER TABLE egcut_epi.analysiscleaning.long_loinc_mapping_blood
ADD COLUMN time_aspect varchar,
ADD COLUMN scale varchar;

ALTER TABLE egcut_epi.analysiscleaning.long_loinc_mapping_urine
ADD COLUMN time_aspect varchar,
ADD COLUMN scale varchar;


update egcut_epi.analysiscleaning.long_loinc_mapping set time_aspect='Pt';
update egcut_epi.analysiscleaning.long_loinc_mapping set scale='Qn';

select count(*) from egcut_epi.analysiscleaning.pre_long_loinc_mapping;
select count(*) from egcut_epi.analysiscleaning.long_loinc_mapping