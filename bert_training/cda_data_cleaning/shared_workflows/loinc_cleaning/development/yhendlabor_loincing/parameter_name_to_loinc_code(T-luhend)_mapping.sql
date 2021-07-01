--------------------------------------------------------------------------------
-- Mapping LOINC codes (= t_lyhend) to work.runfull201903041255_analysis_html
-- As a result of this mapping 73.2% of rows get LOINC codes
-- Note: should be made a function
---------------------------------------------------------------------------------
drop table if exists  analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html;

-- copy from runfull..._html, where a column for loinc is added
create table analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as
  select *
  from work.runfull201903041255_analysis_html;

alter table analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  add column "loinc_code(T-luhend)" varchar;

------------------------------------------------------------------------------------
-- 1. Cleaning the data
------------------------------------------------------------------------------------

------------------------------------------------------------------------------------
-- 1.1 Name cleaning
------------------------------------------------------------------------------------
--a1 ALAT  ->	ALAT
--a20 ASAT ->	ASAT
--a139 Valk->	Valk
--a2041 Hb ->	HB

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substring(parameter_name_raw, 7)
where parameter_name_raw~'^[a]\d{4}';

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substring(parameter_name_raw, 6)
where parameter_name_raw~'^[a]\d{3}';

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substring(parameter_name_raw, 5)
where parameter_name_raw~'^[a]\d{2}';

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substring(parameter_name_raw, 4)
where parameter_name_raw~'^[a]\d{1}';

---------------------------------------------------------------------------------------------
-- 1.2 Removing * from end
---------------------------------------------------------------------------------------------
--Fibrinogeen plasmas* -> Fibrinogeen plasmas
--Kolesterool seerumis* -> Kolesterool seerumis

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set parameter_name_raw = substr(parameter_name_raw, 1, length(parameter_name_raw) - 1)
  where parameter_name_raw like '%*';

---------------------------------------------------------------------------------
 -- 1.3 Removing parentheses
---------------------------------------------------------------------------------
-- NB! Works only for single pair of parentheses
-- NB! Initial `parameter_name` values are kept in the column `parameter_name` and
    -- values with removed parentheses are under `parameter_name_raw`.

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set parameter_name = parameter_name_raw;

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set parameter_name_raw = regexp_replace(parameter_name_raw, '\(.*?\)','')
  where  parameter_name_raw ~ '(.*)\((.*)\)';

-------------------------------------------------------------------------
--1.4 % and # from beginning to end
  ------------------------------------------------------------------------
-- %NEUT -> NEUT%
-- #NEUT -> NEUT#

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set parameter_name_raw = SUBSTRING(parameter_name_raw, 2, 8000) || '%'
  where parameter_name_raw LIKE '\%%';

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set parameter_name_raw = SUBSTRING(parameter_name_raw, 2, 8000)  || '#'
  where parameter_name_raw LIKE '\#%';

--------------------------------------------------------------------------------
-- 1.5 Whitespace unification
--------------------------------------------------------------------------------
-- Replacing multiple whitespaces with one whitespace

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set parameter_name_raw = trim(regexp_replace(parameter_name_raw, '\s+', ' ', 'g'));

-------------------------------------------------------------------------------
-- MAPPING
--------------------------------------------------------------------------------
-- 2. Mapping `parameter_name` -> `t_lyhend`
--------------------------------------------------------------------------------
-- 2.1 Elabor analysis
--------------------------------------------------------------------------------
-- 2.1.1 Mapping using: kasutatav_nimetus, t_lyhend, t_nimetus
--------------------------------------------------------------------------------
--kasutatava nimetusega mapping
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = t_lyhend
  from classifications.elabor_analysis as e
  where t."loinc_code(T-luhend)" is null and
      upper(e.kasutatav_nimetus) = upper(t.parameter_name_raw);

-- unmapped rows now 2798544/4246711 = 65.9%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

-- t_lyhendiga mapping
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = t_lyhend
  from classifications.elabor_analysis as e
  where "loinc_code(T-luhend)" is null and
      upper(e.t_lyhend) = upper(t.parameter_name_raw);

-- unmapped rows now  2538759/4 246 711 59.8%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

-- t_nimetusega mapping
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = t_lyhend
  from classifications.elabor_analysis as e
  where "loinc_code(T-luhend)" is null and
      upper(e.t_nimetus) = upper(t.parameter_name_raw);

-- unmapped rows now 2476782/4 246 711 = 58.3%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

-------------------------------------------------------------------------
-- 2.1.2 Mapping  tlyhend + t_nimetus ->  "loinc_code(T-luhend)"
  ------------------------------------------------------------------------
-- create a mapping table = tlyh_and_t_nimetus_to_tlyhend
drop table if exists analysiscleaning.dirty_code_tlyh_and_t_nimetus_to_tlyhend;
create table analysiscleaning.dirty_code_tlyh_and_t_nimetus_to_tlyhend as
  select t_lyhend || ' ' || t_nimetus AS tlyh_ja_nimetus, t_lyhend as "loinc_code(T-luhend)"
  from classifications.elabor_analysis;

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = a."loinc_code(T-luhend)"
  from analysiscleaning.dirty_code_tlyh_and_t_nimetus_to_tlyhend as a
  where t."loinc_code(T-luhend)" is null and
      upper(a.tlyh_ja_nimetus) = upper(t.parameter_name_raw);

-- unmapped rows now 2459122/4 246 711 = 57.9%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

---------------------------------------------------------------------------------------
-- 2.2 Replacing special characters
---------------------------------------------------------------------------------------
-- ä,ö,õ,ü,Ä,Ö,Õ,Ü' -> 'a,o,o,u,A,O,O,U'
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set parameter_name_raw = translate(parameter_name_raw, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U');

-----------------------------------------------------------------------------------
-- 2.3 Predefined csv file rules
------------------------------------------------------------------------------------------------------------------------
-- 2.3.1 Mapping by parameter_name
-- mapping from dirty_code (parameter_name) -> "loinc_code(T-luhend)"
-- csv files location: /home/anneott/Repos/cda-data-cleaning/analysis_data_cleaning/LOINC_cleaning/development/yhendlabor_loincing/name_cleaning_data/latest_manual_cases.csv
------------------------------------------------------------------------------------------------------------------------
-- creating table out of all the csv mapping files
drop table if exists analysiscleaning.dirty_code_to_t_lyhed_mapping_all;
create table analysiscleaning.dirty_code_to_t_lyhed_mapping_all (
  dirty_code varchar,
  "loinc_code(T-luhend)" varchar
);




insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
  select * from analysiscleaning.latest_name_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
  select * from analysiscleaning.latest_parentheses_one_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
  select * from analysiscleaning.latest_parentheses_two_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
  select * from analysiscleaning.latest_parentheses_three_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
  select * from analysiscleaning.latest_semimanual_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
  select * from analysiscleaning.latest_prefix_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all(dirty_code, "loinc_code(T-luhend)")
  select * from analysiscleaning.latest_manual_cases;
-- for units
--insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, unit, "loinc_code(T-luhend)", loinc_unit)
--select * from analysiscleaning.latest_subspecial_cases;
------------------------------------------------------------------------------------------------------------------------

-- Mapping parameter_name WITHOUT parentheses
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = dc."loinc_code(T-luhend)"
  from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as dc
  where t."loinc_code(T-luhend)" is null and
      upper(dc.dirty_code) = upper(t.parameter_name_raw);

-- unmapped rows now 1280854/4 246 711 = 30.2%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

-- Mapping parameter_name WITH parentheses
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = dc."loinc_code(T-luhend)"
  from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as dc
  where t."loinc_code(T-luhend)" is null and
      upper(dc.dirty_code) = upper(t.parameter_name);

-- unmapped rows now  1255226/4 246 711 = 29.6%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

-------------------------------------------------------------------------------------------------------------------------
-- 2.3.2 Mapping by parameter_name and parameter_unit
-- predefined tabel: latest_subspecial_cases
------------------------------------------------------------------------------------------------------------------------
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = s."loinc_code(T-luhend)"
  from analysiscleaning.latest_subspecial_cases as s
  where upper(s.dirty_code) = upper(t.parameter_name_raw) and
      upper(s.unit) = upper(t.parameter_unit_raw);

-- unmapped rows now  1240030/4 246 711 = 29.1%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

------------------------------------------------------------------------------------------------
-- 3. Mapping t_lyhend using analysis_name
-----------------------------------------------------------------------------------------------

-- Mapping for cases where parameter_name is NULL

-----------------------------------------------------------------------------------------------
-- Repeat step 2.1 on analysis_name
-----------------------------------------------------------------------------------------------
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = t_lyhend
  from classifications.elabor_analysis as e
  where parameter_name_raw is null and
      "loinc_code(T-luhend)" is null and
      upper(e.kasutatav_nimetus) = upper(t.analysis_name_raw);

-- unmapped rows now 1177686/4 246 711 = ..%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = t_lyhend
  from classifications.elabor_analysis as e
  where parameter_name_raw is null and
      "loinc_code(T-luhend)" is null and
      upper(e.t_lyhend) = upper(t.analysis_name_raw);

-- unmapped rows now 1187512/4 246 711 =
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = t_lyhend
  from classifications.elabor_analysis as e
  where parameter_name_raw is null and
      "loinc_code(T-luhend)" is null and
      upper(e.t_nimetus) = upper(t.analysis_name_raw);

-- unmapped rows now 1161162/4 246 711 = 27.3%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

----------------------------------------------------------------------------------------------
-- Repeat step 2.2 on analysis_name
----------------------------------------------------------------------------------------------

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set analysis_name_raw = translate(analysis_name_raw, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U')
  where parameter_name_raw is null;

--------------------------------------------------------------------------------------------
-- Repeat step 2.3 on analysis_name

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
  set "loinc_code(T-luhend)" = pn."loinc_code(T-luhend)"
  from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as pn
  where t.parameter_name_raw is null and
      pn."loinc_code(T-luhend)" is not null and
      t."loinc_code(T-luhend)" is null and
      upper(pn.dirty_code) = upper(t.analysis_name_raw);

-- unmapped rows now 1138682/4 246 711 = 26.8%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where "loinc_code(T-luhend)" is null;


--most frequent unmapped parameter_names
select parameter_name_raw, count(*)
from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null
group by parameter_name_raw
order by count desc;

--most frequent unmapped analysis_names where parameter_name = NULL
select analysis_name_raw, parameter_name_raw, count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null and parameter_name_raw is null
group by parameter_name_raw, analysis_name_raw
order by count desc;