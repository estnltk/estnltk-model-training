-- Teeme mappingu parmeter_code ja t_lyhendi vahel

select parameter_code, t_lyhend, analysis_name, t_nimetus, parameter_name, kasutatav_nimetus,  unit,t_yhik, loinc_no
from analysiscleaning.long_loinc_mapping as l
left join classifications.elabor_analysis as c
    on c.t_lyhend = l.parameter_code
 where t_lyhend is not null
order by t_lyhend asc;

-- A (LLM) -> A (elabor) mapping
drop table if exists analysiscleaning.parameter_code_to_t_lyhend_mapping;
create table analysiscleaning.parameter_code_to_t_lyhend_mapping as
    select distinct parameter_code, t_lyhend
    from analysiscleaning.long_loinc_mapping as l
    left join classifications.elabor_analysis as c
        on c.t_lyhend = l.parameter_code
    where t_lyhend is not null
--order by t_lyhend asc;


-- mapping B (rules from cda-data-cleaning/analysis_data_cleaning/name_cleaning/data ) -> C (t_lyhend)
-- latest_name_cases is for mapping parameter_NAME
-- latest_parentheses_one/two/three_cases.csv peamiselt  parametre_NAME cleaning, veidiveidi ka parameter_code'i
-- long_loinc_mapping parameter_code cleanigus saab t_lyhendeid mappida järgnevate tabelite abil:
  -- latest_subspecial_cases (4 unikaalset rida)
  -- latest_prefix_cases (101 unikaalset rida)
  -- latest_name_cases (11 unikaalset rida)
  -- latest_parentheses_two_cases (4 unikaalset rida)


--latest_subspecial_cases
insert into analysiscleaning.parameter_code_to_t_lyhend_mapping (parameter_code, t_lyhend)
  select distinct parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_subspecial_cases as lsc
    right join analysiscleaning.long_loinc_mapping as llm
      on lsc.dirty_code = llm.parameter_code and
         lsc.unit = llm.unit
    where dirty_code is not null;

--latest_prefix_case
insert into analysiscleaning.parameter_code_to_t_lyhend_mapping (parameter_code, t_lyhend)
    select distinct parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_prefix_cases
    right join analysiscleaning.long_loinc_mapping
      on dirty_code = parameter_code
    where dirty_code is not null;

--latest_name_cases
insert into analysiscleaning.parameter_code_to_t_lyhend_mapping (parameter_code, t_lyhend)
    select distinct parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_name_cases
    right join analysiscleaning.long_loinc_mapping
      on dirty_code = parameter_code
    where dirty_code is not null;

-- latest_parentheses_two_cases
insert into analysiscleaning.parameter_code_to_t_lyhend_mapping (parameter_code, t_lyhend)
    select distinct parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_parentheses_two_cases
    right join analysiscleaning.long_loinc_mapping
      on dirty_code = parameter_code
    where dirty_code is not null;

--analysis name cleaning
/*   select distinct dirty_code, analysis_name_raw, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_name_cases
    right join work.run_ac_html_201901211451_analysis_html_new
      on dirty_code = analysis_name_raw
    where dirty_code is not null;
*/



-- mapping checkup (mapping property for table work.run_ac_html_201901171409_analysis_entry)
-- 1. analysis_html property mapping using LLM (long_loinc_mapping)
-- 2. analysis_html property mapping using t_lyhend

-- entrys kokku 66814 rida
select count(*) from  work.run_ac_html_201901171409_analysis_entry;

-- 1. mapib ära 5232 rea propertyd (66814st)
--create table analysiscleaning.property_mapping_long_loinc_mapping as
select property, entry.parameter_name_raw, llm.parameter_name, entry.* from work.run_ac_html_201901171409_analysis_entry as entry
left join analysiscleaning.long_loinc_mapping as llm
  on llm.analysis_name = entry.analysis_name_raw and
     llm.parameter_name = entry.parameter_name_raw and
     llm.parameter_code = entry.parameter_code_raw
where property is not null;
--entrys pole parameter_named puhastatud!!!!! üleliigsed tühikud
-- llm-s pole paremter_named puhsatatud U-Leu(Leukots��did uriinis)

-- 2. mapib ära 9698 rida, millest küll mõned on '-'(yhendlaboris defineeritud proepry '-')
    -- mapib ära 4324 rida, kui ka propery =  '-' mitte arvestada
-- matching t_lyhend in the new entry with classifications_elabor entry to get properties for entry
create table analysiscleaning.property_mapping_t_lyhend as
select elabor.property, new_entry.* from classifications.elabor_analysis as elabor
right join (
          -- matching parameter_code in entry with t_lyhend
          -- so entry gets one extra column
          select t_lyhend, entry.*
          from work.run_ac_html_201901171409_analysis_entry as entry
                 left join analysiscleaning.parameter_code_to_t_lyhend_mapping as map
                   on entry.parameter_code_raw = map.parameter_code
          --where t_lyhend is not null
          ) as new_entry
    on elabor.t_lyhend = new_entry.t_lyhend
 where new_entry.t_lyhend is not null and property != '-';-- mapib ära 9698 rida, millest küll mõned on '-'


-- võrdlen 1. ja 2. tulemusi

-- peaks tegelt olema mõlemas  tabelis id, mille järgi võrrelda
-- id ühtivad vaid 652 real
select llm.id, tlyh.id
from analysiscleaning.property_mapping_long_loinc_mapping as llm
full outer join
    (select id from analysiscleaning.property_mapping_t_lyhend) as tlyh
    on llm.id = tlyh.id
where tlyh.id is not null and llm.id is not null
--where llm.analysis_name is not null and tlyh.analysis_name is not null;




select pllm.parameter_code_raw, pt.parameter_code_raw,
       pllm.property, pt.property, pllm.analysis_name, pt.analysis_name, pllm.parameter_name,
       pt.parameter_name, pllm.value_raw, pt.value_raw, pllm.parameter_unit_raw, pt.parameter_unit_raw,
       pllm.epi_id, pt.epi_id
from analysiscleaning.property_mapping_long_loinc_mapping as pllm
inner join analysiscleaning.property_mapping_t_lyhend as pt
  on --pllm.property = pt.property and
     pllm.analysis_name IS NOT DISTINCT FROM  pt.analysis_name and -- et null = null oleks ka joinimisel
     pllm.parameter_code_raw IS NOT DISTINCT FROM  pt.parameter_code_raw and
     pllm.epi_id IS NOT DISTINCT FROM  pt.epi_id and
     pllm.value_raw IS NOT DISTINCT FROM  pt.value_raw
;
-----------------------------------------------------------------------------------
--Property mapping analysis_html tabelile

--EI SAA SEST HTML tabelis pole parameter_code'i
select property, html.parameter_name_raw, llm.parameter_name, llm.parameter_code,  html.* from work.run_ac_html_201901211451_analysis_html_new as html
  left join analysiscleaning.long_loinc_mapping as llm
    on llm.analysis_name = html.analysis_name_raw and
     llm.parameter_name = html.parameter_name_raw
     --llm.parameter_code = html.parameter_code_raw
where property is not null;

-- 2. EI SAA SEST POLE PARAMETER_CODE'I
select elabor.property, new_html.* from classifications.elabor_analysis as elabor
right join (
          -- matching parameter_code in entry with t_lyhend
          -- so entry gets one extra column
          select t_lyhend, html.*
          from work.run_ac_html_201901211451_analysis_html_new as html
                 left join analysiscleaning.parameter_code_to_t_lyhend_mapping as map
                   on html.parameter_name_raw = map.parameter_code
          --where t_lyhend is not null
          ) as new_html
    on elabor.t_lyhend = new_html.t_lyhend
 where new_html.t_lyhend is not null and property != '-';-- mapib ära 9698 rida, millest küll mõned on '-'

--kui aga mappida et parameter_name = mappingu tabeli parameter_code

select t_lyhend, html.parameter_name_raw, map.parameter_code, html.*
          from work.run_ac_html_201901211451_analysis_html_new as html
                 left join analysiscleaning.parameter_code_to_t_lyhend_mapping as map
                   on html.parameter_name_raw = map.parameter_code
          where t_lyhend is not null;






-------------------------------------------------------------------------------------
-- ühtlustada parameter code
-- kõik entrys olevad mittenumbrilised parameter_coded
select distinct parameter_code_raw from work.run_ac_html_201901171409_analysis_entry
where not (parameter_code_raw  ~ '^[0-9]*$')
order by parameter_code_raw asc;

-- pingerida
select distinct parameter_code_raw, count(*) from work.run_ac_html_201901171409_analysis_entry
where not (parameter_code_raw  ~ '^[0-9]*$')
group by parameter_code_raw
order by count desc;

-- palju juba neist ühtlustatud parameter_code_mappinguga
select distinct entry.parameter_code_raw, t.parameter_code from work.run_ac_html_201901171409_analysis_entry as entry
left join analysiscleaning.parameter_code_to_t_lyhend_mapping as t
    on entry.parameter_code_raw = t.parameter_code
where not (parameter_code_raw  ~ '^[0-9]*$')
order by parameter_code_raw asc;

--creating table where unifying parameter_code_raw values
-- ex parameter_code_raw --------------------> parameter_code
-- PDW                                         PDW
-- PDW_%                                       PDW
-- PDW_Trombotsüütide_anisotsütoosi_näitaja    PDW
drop table if exists analysiscleaning.parameter_code_unifying;
create table analysiscleaning.parameter_code_unifying as
  select distinct parameter_code_raw, parameter_unit_raw, count(*) from work.run_ac_html_201901171409_analysis_entry
  where not (parameter_code_raw  ~ '^[0-9]*$')
  group by parameter_code_raw, parameter_unit_raw
  order by parameter_code_raw asc;

select distinct e2.*, e1.parameter_unit_raw from work.run_ac_html_201901171409_analysis_entry as e1
right join (
          select distinct parameter_code_raw, count(*) from work.run_ac_html_201901171409_analysis_entry
          where not (parameter_code_raw  ~ '^[0-9]*$')
          group by parameter_code_raw
          order by parameter_code_raw asc) as e2
on e1.parameter_code_raw = e2.parameter_code_raw;

/*
select distinct analysis_name, count(*) from work.run_ac_html_201901171409_analysis_entry
    group by analysis_name

select distinct parameter_name, count(*) from work.run_ac_html_201901171409_analysis_entry
    group by parameter_name


select * from classifications.elabor_analysis
    where lower(kasutatav_nimetus) ~ '^baso'

select *
from classifications.elabor_analysis
where component = 'Basophils/100 leukocytes';
*/


---puhastatud koodide veerg
ALTER TABLE analysiscleaning.parameter_code_unifying
  ADD column parameter_code_new VARCHAR,
  ADD column exists_in_cleaning_rules VARCHAR;
  --ADD column parameter_unit VARCHAR; -- vajalik latest_subspecial_cases reeglite rakendamiseks

-- kas parameter_code on defineeritud olemasolevates reeglites või mitte
-- kui parameter_code on juba defineeritud csv reeglites,
-- siis veerg exists_in_cleaning_rules peaks olema True ja neid enam puhastada pole vaja
-- lisame ka puhastatud nime
update analysiscleaning.parameter_code_unifying
set exists_in_cleaning_rules = True
where (parameter_code_raw, not null) in
    (select parameter_code_raw, "loinc_code(T-luhend)" from analysiscleaning.latest_prefix_cases
    right join analysiscleaning.parameter_code_unifying
    on dirty_code = parameter_code_raw
    where dirty_code is not null);

update analysiscleaning.parameter_code_unifying
set exists_in_cleaning_rules = True --, parameter_code_new =
where parameter_code_raw in
    (select parameter_code_raw from analysiscleaning.latest_parentheses_one_cases
    right join analysiscleaning.parameter_code_unifying
    on dirty_code = parameter_code_raw
    where dirty_code is not null);

update analysiscleaning.parameter_code_unifying
set exists_in_cleaning_rules = True --, parameter_code_new =
where parameter_code_raw in
    (select parameter_code_raw from analysiscleaning.latest_parentheses_two_cases
    right join analysiscleaning.parameter_code_unifying
    on dirty_code = parameter_code_raw
    where dirty_code is not null);

update analysiscleaning.parameter_code_unifying
set exists_in_cleaning_rules = True --, parameter_code_new =
where parameter_code_raw in
    (select parameter_code_raw from analysiscleaning.latest_parentheses_three_cases
    right join analysiscleaning.parameter_code_unifying
    on dirty_code = parameter_code_raw
    where dirty_code is not null);

update analysiscleaning.parameter_code_unifying
set exists_in_cleaning_rules = True --, parameter_code_new =
where parameter_code_raw in
    (select parameter_code_raw from analysiscleaning.latest_subspecial_cases as l
    right join analysiscleaning.parameter_code_unifying as u
    on l.dirty_code = u.parameter_code_raw and l.unit = u.parameter_unit_raw
    where dirty_code is not null);

update analysiscleaning.parameter_code_unifying
set exists_in_cleaning_rules = True --, parameter_code_new =
where parameter_code_raw in
    (select parameter_code_raw from analysiscleaning.latest_name_cases
    right join analysiscleaning.parameter_code_unifying
    on dirty_code = parameter_code_raw
    where dirty_code is not null);

update analysiscleaning.parameter_code_unifying
set exists_in_cleaning_rules = True --, parameter_code_new =
where parameter_code_raw in
    (select parameter_code_raw from analysiscleaning.latest_semimanual_cases
    right join analysiscleaning.parameter_code_unifying
    on dirty_code = parameter_code_raw
    where dirty_code is not null);

--muudel juhtudel false
update analysiscleaning.parameter_code_unifying
set exists_in_cleaning_rules = False
where exists_in_cleaning_rules is null;


UPDATE analysiscleaning.parameter_code_unifying
SET parameter_code_new = 'Alaniini aminotransferaas' WHERE parameter_code_raw like '%Alaniini%';

UPDATE analysiscleaning.parameter_code_unifying
SET parameter_code_new = 'S,P-ALP'
where (parameter_code_raw like '%fosfataas%' or parameter_code_raw like '%ALP%')
  and parameter_code_raw not like '%Leelisfosfataas%';

UPDATE analysiscleaning.parameter_code_unifying
SET parameter_code_new = 'P-Amyl'
where parameter_code_raw like '%P-Amyl%' or parameter_code_raw like '%Amülaas_plasmas%';

UPDATE analysiscleaning.parameter_code_unifying
SET parameter_code_new = 'S-Amyl'
where parameter_code_raw like '%S-Amyl%' or parameter_code_raw like '%Amülaas_seerumis%';

-- sama mis ASAT, aga ei tea, kas p või s
update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'Aspartaadi aminotransferaas'
where  parameter_code_raw like '%Aspartaadi_aminotransferaas%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'P-ASAT'
where  parameter_code_raw like '%P-ASAT%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'S-ASAT'
where  parameter_code_raw like '%S-ASAT%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'BASO#'
where  parameter_code_raw like '%baso#%' or parameter_code_raw like '%BASO#%' or
       parameter_code_raw like '%BASO_Basofiilide_absoluutarv%'

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'BASO%'
where  parameter_code_raw like '%baso\%%' or parameter_code_raw like '%BASO\%%' or
       parameter_code_raw like 'B-Basofüülid_\%'

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'BASO%'
where  parameter_code_raw like '%baso\%%' or parameter_code_raw like '%BASO\%%' or
       parameter_code_raw like 'B-Basofüülid_\%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'B-ABO'
where   parameter_code_raw like '%B-ABO%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'B-aRBC'
where   parameter_code_raw like '%B-aRBC%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'B-DAT'
where   parameter_code_raw like '%B-DAT%';

--!!!!!!!!!!!!!! KAS ÜLDSITUS ÕIGE ÜLDSE
update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'aB'
where   parameter_code_raw like '%aB-%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'S-CRP'
where   parameter_code_raw like '%S-CRP%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'S-Na'
where   parameter_code_raw like '%S-Na%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'S-K'
where   parameter_code_raw like '%S-K%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'S-Crea'
where   parameter_code_raw like 'S-Crea%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'P-Urea'
where   parameter_code_raw like 'P-Urea%';

update analysiscleaning.parameter_code_unifying
set parameter_code_new = 'S-Urea'
where   parameter_code_raw like 'S-Urea%';


select * from analysiscleaning.parameter_code_unifying
where  parameter_code_raw like 'S-Urea%'-- or parameter_code_raw like '%BASO%' or parameter_code_raw like '%Baso%'













---------------------------------------------------------------------

select distinct parameter_code_raw, count(*)
from work.run_ac_html_201901171409_analysis_entry
where not (parameter_code_raw  ~ '^[0-9]*$') and parameter_code_raw like '%(%'
group by parameter_code_raw
order by parameter_code_raw asc;


-- OLEMASOLEVATE REEGLITE KASUTUS PARAMETER CODE ÜHTLUSTAMISEKS
-- Ühtlustab küll t-lühendiks....
--latest_prefix_case
select distinct e.parameter_code_raw as e_parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_prefix_cases
    right join work.run_ac_html_201901171409_analysis_entry as e
      on dirty_code = e.parameter_code_raw
    where dirty_code is not null;

--latest_subspecial_cases
  select distinct parameter_code_raw, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_subspecial_cases as lsc
    right join work.run_ac_html_201901171409_analysis_entry as e
      on lsc.dirty_code = e.parameter_code_raw and
         lsc.unit = e.parameter_unit
    where dirty_code is not null;
--latest_name_cases
select distinct e.parameter_code_raw as e_parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_name_cases
    right join work.run_ac_html_201901171409_analysis_entry as e
      on dirty_code = e.parameter_code_raw
    where dirty_code is not null;
--latest_prefix_cases
select distinct e.parameter_code_raw as e_parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_prefix_cases
    right join work.run_ac_html_201901171409_analysis_entry as e
      on dirty_code = e.parameter_code_raw
    where dirty_code is not null;
--latest_parentheses_one_cases NULL
select distinct e.parameter_code_raw as e_parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_parentheses_one_cases
    right join work.run_ac_html_201901171409_analysis_entry as e
      on dirty_code = e.parameter_code_raw
    where dirty_code is not null;
--latest_parentheses_one_cases NULL
select distinct e.parameter_code_raw as e_parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_parentheses_two_cases
    right join work.run_ac_html_201901171409_analysis_entry as e
      on dirty_code = e.parameter_code_raw
    where dirty_code is not null;
--latest_parentheses_one_cases NULL
select distinct e.parameter_code_raw as e_parameter_code, "loinc_code(T-luhend)" as t_lyhend from analysiscleaning.latest_parentheses_three_cases
    right join work.run_ac_html_201901171409_analysis_entry as e
      on dirty_code = e.parameter_code_raw
    where dirty_code is not null;


!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1
select distinct analysis_name from work.run_ac_html_201901171409_analysis_entry
where not (analysis_name  ~ '^[0-9]*$')
order by analysis_name asc;

select *
from classifications.elabor_analysis
where kasutatav_nimetus ~ '^Hemogramm 5-osalise'; --loinc_no = '57021-8';

select *
from analysiscleaning.long_loinc_mapping
where analysis_name ~ '^Hemogramm 5-osalise'; --loinc_no = '57021-8';

----------------------------------------------------------------
-- Analysis_namedele panna vastavusse t_nimetused/kasutatavad_nimetused
select t_nimetus from classifications.elabor_analysis
where vastuskood = 'Paneel'
group by t_nimetus
order by t_nimetus asc;

select kasutatav_nimetus from classifications.elabor_analysis
where vastuskood = 'Paneel'
group by kasutatav_nimetus
order by kasutatav_nimetus asc;

select t_nimetus from classifications.elabor_analysis
where vastuskood = 'Paneel';

-- populaarseimad paneelid llm-s
select analysis_name, count(*) from analysiscleaning.long_loinc_mapping
group by analysis_name
order by count desc;

select analysis_name, parameter_name, count(*) from analysiscleaning.long_loinc_mapping
where analysis_name like '%Perika%'
group by analysis_name, parameter_name
order by count desc;
-----
-- palju llm ridadest saab mapitud t_nimetus_to_analysis_name_mapping?
select  entry.analysis_name, t.t_nimetus from work.run_ac_html_201901171409_analysis_entry as entry
left join analysiscleaning.t_nimetus_to_analysis_name_mapping as t
on entry.analysis_name = t.analysis_name
where t.t_nimetus is not null;
--group by  llm.analysis_name
--order by count desc ;
-- llm's on 12743 rida
-- t_nimetuse mappinguga katame ära 2296 rida
--array_agg((t.t_nimetus, t.analysis_name)),


-------------------------------------------------------------------


-- üksikute võrdlus

select analysis_name, parameter_name, parameter_code_raw
from work.run_ac_html_201901171409_analysis_entry
where epi_id = '6862747';

select *
from analysiscleaning.long_loinc_mapping
where analysis_name = 'Alaniini aminotransferaas' and parameter_code = 'S-ALT';

select *
from analysiscleaning.property_mapping_long_loinc_mapping
where epi_id = '6862747';

select *
from analysiscleaning.property_mapping_t_lyhend
where epi_id = '6862747';


-- vaatame millised epi_id ära matchitatkse
-- llm mapib ära 117, tlyh mapib ära 131, ühisosa on 37 epi_id
select distinct llm.epi_id, tlyh.epi_id from analysiscleaning.property_mapping_long_loinc_mapping as llm
full outer join
      (select distinct epi_id from analysiscleaning.property_mapping_t_lyhend)  as tlyh
on llm.epi_id = tlyh.epi_id
--where llm.epi_id is not null and tlyh.epi_id is not null;--kumbki pole null
