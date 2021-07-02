-- NB! VAJA PREFIXEID

drop table if exists analysiscleaning.analysis_html_loinced_runfull20192705;
create table analysiscleaning.analysis_html_loinced_runfull20192705 as
  select row_nr, epi_id, epi_type, parse_type, panel_id, analysis_name_raw, parameter_name_raw, parameter_unit_raw, reference_values_raw, effective_time_raw, value_raw
  from work.runfull201903041255_analysis_html;

alter table analysiscleaning.analysis_html_loinced_runfull20192705
    add column loinc_code varchar,
    add column t_lyhend varchar;

------------------------------------------------------------------------------------
-- 1. CLEANING PARAMETER_NAME
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
-- 1.1 Name cleaning
------------------------------------------------------------------------------------
--a1 ALAT  ->	ALAT
--a20 ASAT ->	ASAT
--a139 Valk->	Valk
--a2041 Hb ->	HB

update analysiscleaning.analysis_html_loinced_runfull20192705
set parameter_name_raw = substring(parameter_name_raw, 7)
where parameter_name_raw~'^[a]\d{4}';

update analysiscleaning.analysis_html_loinced_runfull20192705
set parameter_name_raw = substring(parameter_name_raw, 6)
where parameter_name_raw~'^[a]\d{3}';

update analysiscleaning.analysis_html_loinced_runfull20192705
set parameter_name_raw = substring(parameter_name_raw, 5)
where parameter_name_raw~'^[a]\d{2}';

update analysiscleaning.analysis_html_loinced_runfull20192705
set parameter_name_raw = substring(parameter_name_raw, 4)
where parameter_name_raw~'^[a]\d{1}';

create table analysiscleaning.runfull201903041255_analysis_html as
select * from work.runfull201903041255_analysis_html;



---------------------------------------------------------------------------------------------
-- 1.2 Removing * from end
---------------------------------------------------------------------------------------------
--Fibrinogeen plasmas* -> Fibrinogeen plasmas
--Kolesterool seerumis* -> Kolesterool seerumis

update analysiscleaning.analysis_html_loinced_runfull20192705
  set parameter_name_raw = substr(parameter_name_raw, 1, length(parameter_name_raw) - 1)
  where parameter_name_raw like '%*';

---------------------------------------------------------------------------------
 -- 1.3 Removing parentheses
---------------------------------------------------------------------------------
-- NB! Works only for single pair of parentheses
-- NB! Initial `parameter_name` values are kept in the column `parameter_name` and
    -- values with removed parentheses are under `parameter_name_raw`.

--update analysiscleaning.analysis_html_loinced_runfull201904041255
--  set parameter_name = parameter_name_raw;

update analysiscleaning.analysis_html_loinced_runfull20192705
  set parameter_name_raw = regexp_replace(parameter_name_raw, '\(.*?\)','')
  where  parameter_name_raw ~ '(.*)\((.*)\)';

-------------------------------------------------------------------------
--1.4 % and # from beginning to end
  ------------------------------------------------------------------------
-- %NEUT -> NEUT%
-- #NEUT -> NEUT#

update analysiscleaning.analysis_html_loinced_runfull20192705
  set parameter_name_raw = SUBSTRING(parameter_name_raw, 2, 8000) || '%'
  where parameter_name_raw LIKE '\%%';

update analysiscleaning.analysis_html_loinced_runfull20192705
  set parameter_name_raw = SUBSTRING(parameter_name_raw, 2, 8000)  || '#'
  where parameter_name_raw LIKE '\#%';

--------------------------------------------------------------------------------
-- 1.5 Whitespace unification
--------------------------------------------------------------------------------
-- Replacing multiple whitespaces with one whitespace

update analysiscleaning.analysis_html_loinced_runfull20192705
  set parameter_name_raw = trim(regexp_replace(parameter_name_raw, '\s+', ' ', 'g'));




-----------------------------------------------------------------------------------------------------------------------
-- APPLYING LOINC MAPPING
-----------------------------------------------------------------------------------------------------------------------
-- ELABOR
-----------------------------------------------------------------------------------------------------------------------
-- määrame loinci LOINC_cleaning/data all olevate csv failide põhjal

-- parameter_name ->_loinc mapping
update analysiscleaning.analysis_html_loinced_runfull20192705 as a
  set loinc_code = e.loinc_code, t_lyhend = e.t_lyhend
  from analysiscleaning.elabor_parameter_name_to_loinc_mapping as e
  where upper(a.parameter_name_raw) = upper(e.parameter_name);

-- ridu kokku 5457897
-- loincitud 950988/5457897
-- loincitud clean PN 1924030/5457897
select count(*) from analysiscleaning.analysis_html_loinced_runfull20192705
where loinc_code is not null;

-- parameter_name and parameter_unit ->_loinc mapping
update analysiscleaning.analysis_html_loinced_runfull20192705 as a
  set loinc_code = e.loinc_code, t_lyhend = e.t_lyhend
  from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping e
  where upper(a.parameter_name_raw) = upper(e.parameter_name) and upper(a.parameter_unit_raw) = upper(e.t_yhik);

-- loincitud 1378529/5457897=25%
-- loincitud clean PN 1999393/5457897
select count(*) from analysiscleaning.analysis_html_loinced_runfull20192705
where loinc_code is not null;


-----------------------------------------------------------------------------------------------------------------------
-- PREDEFINED CSV
-----------------------------------------------------------------------------------------------------------------------
update analysiscleaning.analysis_html_loinced_runfull20192705
  set parameter_name_raw = translate(parameter_name_raw, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U');

-- parameter_name -> loinc mapping
update analysiscleaning.analysis_html_loinced_runfull20192705 as a
  set loinc_code = p.loinc_code,  t_lyhend = p.t_lyhend
  from analysiscleaning.run_ac_201905270927parameter_name_to_loinc_mapping as p
  where a.loinc_code is null and upper(a.parameter_name_raw) = upper(p.parameter_name);

-- loincitud 2451436/5457897
-- loincitud clean PN 3527349/5457897 = 64%
select count(*) from analysiscleaning.analysis_html_loinced_runfull20192705
where loinc_code is not null;


--parameter_name and parameter_unit -> loinc mapping
update analysiscleaning.analysis_html_loinced_runfull20192705 as a
  set loinc_code = p.loinc_code, t_lyhend = p.t_lyhend
  from analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping as p
  where upper(a.parameter_name_raw) = upper(p.parameter_name) and upper(a.parameter_unit_raw) = upper(p.parameter_unit);

-- loincitud 2473898/5457897=45%
-- loincitud clean PN 3546008/5457897=64%
--loincitud 3580827/5457897 = 65%
-- pärast VALE mappingute eemaldamist 3567252/
-- 3552907
select count(*) from analysiscleaning.analysis_html_loinced_runfull20192705
where loinc_code is not null;


-- mappimata
select parameter_name_raw, upper(parameter_unit_raw) as parameter_unit_raw, count(*) from analysiscleaning.analysis_html_loinced_runfull20192705
where loinc_code is null
group by parameter_name_raw, upper(parameter_unit_raw)
order by count desc;


select parameter_name_raw, count(*) from analysiscleaning.runfull201903041255_analysis_html_loinced
where loinc_code is null
group by parameter_name_raw
order by count desc ;

select count(*) from analysiscleaning.runfull201903041255_analysis_html_loinced
where loinc_code is not null;