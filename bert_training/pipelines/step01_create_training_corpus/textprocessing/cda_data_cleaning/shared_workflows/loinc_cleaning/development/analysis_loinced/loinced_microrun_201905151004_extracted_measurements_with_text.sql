--12550 rida kokku

drop table if exists analysiscleaning.microrun_201905151004_extracted_measurements_with_text;
create table analysiscleaning.microrun_201905151004_extracted_measurements_with_text as
    select * from egcut_epi.work.microrun_201905151004_extracted_measurements_with_text;
-- 8591 juba loinc code küljes

update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
  set oject = (regexp_replace(m.oject,' ', ''));

update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
set loinc_code = e.loinc_code, t_lyhend = e.t_lyhend
from analysiscleaning.elabor_parameter_name_to_loinc_mapping as e
where lower(m.oject) = lower(e.parameter_name);

update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
set loinc_code = p.loinc_code, t_lyhend = p.t_lyhend
from analysiscleaning.parameter_name_to_loinc_mapping as p
where lower(m.oject) = lower(p.parameter_name);

update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
set loinc_code = e.loinc_code, t_lyhend = e.t_lyhend
from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping as e
where lower(m.oject) = lower(e.parameter_name) and m.unit = e.t_yhik;

update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
set loinc_code = p.loinc_code, t_lyhend = p.t_lyhend
from analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping as p
where lower(m.oject) = lower(p.parameter_name) and m.unit = p.parameter_unit;
--10 136 real loinc_code küljes

select * from classifications.elabor_analysis