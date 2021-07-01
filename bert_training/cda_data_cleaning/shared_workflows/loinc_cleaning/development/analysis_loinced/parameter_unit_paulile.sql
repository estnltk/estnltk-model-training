select distinct * from analysiscleaning.elabor_parameter_name_to_loinc_mapping;
--18124
select distinct * from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping;
-- 103
select distinct * from analysiscleaning.parameter_name_to_loinc_mapping;
--1982
select distinct * from analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping;
--100
select * from classifications.elabor_analysis
where t_lyhend like '%S-d1 IgE%'

--12550 rida kokku

drop table if exists analysiscleaning.microrun_201905151004_extracted_measurements_with_text;
create table analysiscleaning.microrun_201905151004_extracted_measurements_with_text as
    select * from egcut_epi.work.microrun_201905151004_extracted_measurements_with_text;

update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
  set oject = (regexp_replace(m.oject,' ', ''));
-- 8591 juba mapitud


update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
set loinc_code = e.loinc_code, t_lyhend = e.t_lyhend
from analysiscleaning.elabor_parameter_name_to_loinc_mapping as e
where lower(m.oject) = lower(e.parameter_name);

select count(*) from analysiscleaning.microrun_201905151004_extracted_measurements_with_text
where loinc_code is not null;
-- mapitud 10136

/*
select w.oject, w.unit, e.t_lyhend, e.loinc_code
from egcut_epi.work.microrun_201905151004_extracted_measurements_with_text as w
left join analysiscleaning.elabor_parameter_name_to_loinc_mapping as e
on w.oject = e.parameter_name
where e.loinc_code is not null;
-- 6896 mapitud
 */

update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
set loinc_code = p.loinc_code, t_lyhend = p.t_lyhend
from analysiscleaning.parameter_name_to_loinc_mapping as p
where lower(m.oject) = lower(p.parameter_name);

select count(*) from analysiscleaning.microrun_201905151004_extracted_measurements_with_text
where loinc_code is not null;
--10136

/*
select w.oject, w.unit, e.t_lyhend, e.loinc_code
from egcut_epi.work.microrun_201905151004_extracted_measurements_with_text as w
left join analysiscleaning.parameter_name_to_loinc_mapping as e
on w.oject = e.parameter_name
where e.loinc_code is not null;
-- 3264
*/
update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
set loinc_code = e.loinc_code, t_lyhend = e.t_lyhend
from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping as e
where lower(m.oject) = lower(e.parameter_name) and m.unit = e.t_yhik;

select count(*) from analysiscleaning.microrun_201905151004_extracted_measurements_with_text
where loinc_code is not null;
-- 10136

/*
select w.oject, w.unit, e.t_lyhend, e.loinc_code
from egcut_epi.work.microrun_201905151004_extracted_measurements_with_text as w
left join analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping as e
on w.oject = e.parameter_name and w.unit = e.t_yhik
where e.loinc_code is not null;
--215
*/


update analysiscleaning.microrun_201905151004_extracted_measurements_with_text as m
set loinc_code = p.loinc_code, t_lyhend = p.t_lyhend
from analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping as p
where lower(m.oject) = lower(p.parameter_name) and m.unit = p.parameter_unit;

select count(*) from analysiscleaning.microrun_201905151004_extracted_measurements_with_text
where loinc_code is not null;
--10136

select oject, count(*) from analysiscleaning.microrun_201905151004_extracted_measurements_with_text
where loinc_code is null
group by oject
order by count desc ;

select *
from classifications.elabor_analysis where t_lyhend like '%INR%';



select * from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping
where lower(parameter_name) like '%fr%';

select * from analysiscleaning.elabor_parameter_name_to_loinc_mapping
where lower(parameter_name) like '%fr';

select * from analysiscleaning.parameter_name_to_loinc_mapping
where lower(parameter_name) like '%fr%';

select * from analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping
where lower(parameter_name) like '%fr%';

select *
from analysiscleaning.analysis_html_loinced2_runfull201904041255
where analysis_name_raw like '%RR%';

/*
select w.oject, w.unit, e.t_lyhend, e.loinc_code
from egcut_epi.work.microrun_201905151004_extracted_measurements_with_text as w
left join analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping as e
on w.oject = e.parameter_name and w.unit = e.parameter_unit
where e.loinc_code is not null;
--1407

 */