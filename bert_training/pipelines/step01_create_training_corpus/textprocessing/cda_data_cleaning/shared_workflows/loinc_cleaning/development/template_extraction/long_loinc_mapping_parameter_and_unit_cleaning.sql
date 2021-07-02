create table analysiscleaning.long_loinc_mapping_removing_anonym as
select * from analysiscleaning.long_loinc_mapping
where parameter_name like '%ANONYM%' or unit like '%ANONYM%'

-- 144 rida, kus unit või parameter on ANONYM
select * from analysiscleaning.long_loinc_mapping_removing_anonym
where parameter_name like '%ANONYM%' or unit like '%ANONYM%';

-- asendame ANONYMi tühja stringiga
update analysiscleaning.long_loinc_mapping_removing_anonym set parameter_name = replace(parameter_name, '<ANONYM>', '');
update analysiscleaning.long_loinc_mapping_removing_anonym set unit = replace(unit, '<ANONYM>', '');

-- kontroll
select * from analysiscleaning.long_loinc_mapping_removing_anonym
where parameter_name like '%ANONYM%';

--võrdlus
select * from analysiscleaning.long_loinc_mapping_removing_anonym
where unit like '%ANONYM%';


select * from analysiscleaning.mapped_all_templates
where parameter_name like '%ANONYM%'