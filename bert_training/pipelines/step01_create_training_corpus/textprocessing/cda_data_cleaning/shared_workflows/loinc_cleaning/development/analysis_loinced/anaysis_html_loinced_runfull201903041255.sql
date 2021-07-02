drop table if exists analysiscleaning.analysis_html_loinced2_runfull201904041255;
create table analysiscleaning.analysis_html_loinced2_runfull201904041255 as
  select row_nr, epi_id, epi_type, parse_type, panel_id, analysis_name_raw, parameter_name_raw, parameter_unit_raw, reference_values_raw, effective_time_raw, value_raw
  from work.runfull201903041255_analysis_html_new;

alter table analysiscleaning.analysis_html_loinced2_runfull201904041255
    add column loinc_code varchar,
    add column t_lyhend varchar;

-----------------------------------------------------------------------------------------------------------------------
--PUHASTAMSIE SAMM
-----------------------------------------------------------------------------------------------------------------------
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

update analysiscleaning.analysis_html_loinced2_runfull201904041255
set parameter_name_raw = substring(parameter_name_raw, 7)
where parameter_name_raw~'^[a]\d{4}';

update analysiscleaning.analysis_html_loinced2_runfull201904041255
set parameter_name_raw = substring(parameter_name_raw, 6)
where parameter_name_raw~'^[a]\d{3}';

update analysiscleaning.analysis_html_loinced2_runfull201904041255
set parameter_name_raw = substring(parameter_name_raw, 5)
where parameter_name_raw~'^[a]\d{2}';

update analysiscleaning.analysis_html_loinced2_runfull201904041255
set parameter_name_raw = substring(parameter_name_raw, 4)
where parameter_name_raw~'^[a]\d{1}';

---------------------------------------------------------------------------------------------
-- 1.2 Removing * from end
---------------------------------------------------------------------------------------------
--Fibrinogeen plasmas* -> Fibrinogeen plasmas
--Kolesterool seerumis* -> Kolesterool seerumis

update analysiscleaning.analysis_html_loinced2_runfull201904041255
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

update analysiscleaning.analysis_html_loinced2_runfull201904041255
  set parameter_name_raw = regexp_replace(parameter_name_raw, '\(.*?\)','')
  where  parameter_name_raw ~ '(.*)\((.*)\)';

-------------------------------------------------------------------------
--1.4 % and # from beginning to end
  ------------------------------------------------------------------------
-- %NEUT -> NEUT%
-- #NEUT -> NEUT#

update analysiscleaning.analysis_html_loinced2_runfull201904041255
  set parameter_name_raw = SUBSTRING(parameter_name_raw, 2, 8000) || '%'
  where parameter_name_raw LIKE '\%%';

update analysiscleaning.analysis_html_loinced2_runfull201904041255
  set parameter_name_raw = SUBSTRING(parameter_name_raw, 2, 8000)  || '#'
  where parameter_name_raw LIKE '\#%';

--------------------------------------------------------------------------------
-- 1.5 Whitespace unification
--------------------------------------------------------------------------------
-- Replacing multiple whitespaces with one whitespace

update analysiscleaning.analysis_html_loinced2_runfull201904041255
  set parameter_name_raw = trim(regexp_replace(parameter_name_raw, '\s+', ' ', 'g'));




-----------------------------------------------------------------------------------------------------------------------
--LOINCIMISE SAMM
-----------------------------------------------------------------------------------------------------------------------


  -- ELABOR
-- määrame loinci LOINC_cleaning/data all olevate failide abil

update analysiscleaning.analysis_html_loinced2_runfull201904041255 as a
  set loinc_code = e.loinc_code, t_lyhend = e.t_lyhend
  from analysiscleaning.elabor_parameter_name_to_loinc_mapping as e
  where upper(a.parameter_name_raw) = upper(e.parameter_name);

-- ridu kokku 5457897
-- loincitud 950988/5457897
-- loincitud clean PN 1924030/5457897
select count(*) from analysiscleaning.analysis_html_loinced2_runfull201904041255
where loinc_code is not null;

-- UNITI järgi
update analysiscleaning.analysis_html_loinced2_runfull201904041255 as a
  set loinc_code = e.loinc_code, t_lyhend = e.t_lyhend
  from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping e
  where upper(a.parameter_name_raw) = upper(e.parameter_name) and upper(a.parameter_unit_raw) = upper(e.t_yhik);

-- loincitud 1378529/5457897=25%
-- loincitud clean PN 1999393/5457897
select count(*) from analysiscleaning.analysis_html_loinced2_runfull201904041255
where loinc_code is not null;


-- CSV FAILIDE MAPPING
update analysiscleaning.analysis_html_loinced2_runfull201904041255
  set parameter_name_raw = translate(parameter_name_raw, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U');


update analysiscleaning.analysis_html_loinced2_runfull201904041255 as a
  set loinc_code = p.loinc_code,  t_lyhend = p.t_lyhend
  from analysiscleaning.parameter_name_to_loinc_mapping as p
  where a.loinc_code is null and upper(a.parameter_name_raw) = upper(p.parameter_name);

-- loincitud 2451436/5457897
-- loincitud clean PN 3527349/5457897 = 64%
select count(*) from analysiscleaning.analysis_html_loinced2_runfull201904041255
where loinc_code is not null;

update analysiscleaning.analysis_html_loinced2_runfull201904041255 as a
  set loinc_code = p.loinc_code, t_lyhend = p.t_lyhend
  from analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping as p
  where upper(a.parameter_name_raw) = upper(p.parameter_name) and upper(a.parameter_unit_raw) = upper(p.parameter_unit);

-- loincitud 2473898/5457897=45%
-- loincitud clean PN 3546008/5457897=64%
--loincitud 3580827/5457897 = 65%
-- pärast VALE mappingute eemaldamist 3567252/
-- 3552907
select count(*) from analysiscleaning.analysis_html_loinced2_runfull201904041255
where loinc_code is not null;


-- mappimata
select parameter_name_raw, upper(parameter_unit_raw), count(*) from analysiscleaning.analysis_html_loinced2_runfull201904041255
where loinc_code is null
group by parameter_name_raw, upper(parameter_unit_raw)
order by count desc;


-----------------------------------------------------------------------------------------------------------------------
-- unitite kontroll
-----------------------------------------------------------------------------------------------------------------------
--LOINCI UNITITE MÄÄRAMINE
--lisame puhastatud loinc_unitid
drop table if exists analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2;

create table analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2 as
  select parameter_unit_raw as loinc_unit,  a.* from analysiscleaning.analysis_html_loinced2_runfull201904041255 as a;

-- kui loinc unitist ka cleanitud versioon, siis võtame selle kasutusele
update analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2 as a
set loinc_unit = p.loinc_unit
from analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
where (a.parameter_unit_raw) = (p.parameter_unit);

/*
  select p.loinc_unit, a.* from analysiscleaning.analysis_html_loinced2_runfull201904041255 as a
    left join analysiscleaning.parameter_unit_to_loinc_unit_mapping  as p on
    (a.parameter_unit_raw) = (p.parameter_unit);
*/

--1908567
--3549330
select count(*) from analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2
where loinc_unit is not null;

-- EI JOOKSE LÄBI!!!
--kui loinc unit jäi tühjaks, siis lisame sinna tabalise parameter_name_raw
/*update analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit
  set loinc_unit = parameter_unit_raw
  where parameter_unit_raw is not null and
        parameter_unit_raw != '-' and
        loinc_unit is null;*/

-- 176 pn, unit1, unit2 konflikti!!!

-- loinc_unit on saadud: PN -> PU -> loinc_unit
-- loinc_no on saadud: PN -> LOINC code -> loinc unit
-- võrdele, kas tekib vastuolusid

-- pole nii hea võrdlus!!
-- kokku 67 erienvat sellist unitite paari
-- 62
select e.t_yhik, a.loinc_unit, count(*), array_agg(distinct (a.t_lyhend, a.parameter_name_raw))
  from analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit as a
  left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
  where upper(a.loinc_unit) != upper(e.t_yhik)
    group by upper(t_yhik), upper(a.loinc_unit)
  order by count desc;

--parem võrdlus
-- kokku konfliktseid ridu 51 506 / 5457897
-- kokku 46177
-- kokku 34393 / 5457897 = 6%
--       33707
  --     31337
  --     23561
-- konfliktsed parameter_name
select count(*)--distinct a.parameter_name_raw, a.loinc_unit, e.t_yhik
    from analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2 as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where upper(a.loinc_unit) is not distinct from upper(e.t_yhik);


--olukorrad, kus kõik HÄSTI ehk loinc_unit = tyhik
--1817845
--1817825
--1707605
--1703508
select count(*)
    from analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2 as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where upper(a.loinc_unit) is not distinct from upper(e.t_yhik);


-- KONFLIKTSED uniti PAARID parameter_name järgi!!!
-- 178
-- 176
-- 129
-- 126
-- count 505 049 -> 328 963 -> 245 872
select distinct parameter_name_raw, loinc_unit as unit_based_on_parameter_unit, t_yhik as unit_based_on_loinc_code, count(*), array_agg(distinct loinc_code)
from analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2 as a
left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
where upper(a.loinc_unit) != upper(e.t_yhik)
group by parameter_name_raw, loinc_unit, t_yhik
order by count desc;

select * from analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2
where loinc_unit like '10*9/L';

select * from analysiscleaning.parameter_unit_to_loinc_unit_mapping
where parameter_unit like '%10*9/L%';

-- read, kus PN -> PU -> Loinc uniti on NULL
-- aga PN -> Loinc_code -> Loinc unit POLE null
--1 507 757
--1 506 526
-- 1626262 -> 1049120
select count(*)
    from analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2 as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where a.loinc_unit is null and e.t_yhik is not null;

-- read, kus PN -> PU -> Loinc uniti POLE null
-- aga PN -> Loinc_code -> Loinc unit on NULL
-- 547 977
-- 548 683
--1254737
select sum(count) from(
select parameter_name_raw, a.loinc_unit, e.t_yhik, array_agg(distinct loinc_code), count(*)
    from analysiscleaning.analysis_html_loinced_runfull201904041255_loinc_unit2 as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where a.loinc_unit is not null and e.t_yhik is null and
          --loinc_code is nogitt null -- võib välja jätta
          loinc_unit != '-' and
          loinc_code is not null
group by parameter_name_raw, a.loinc_unit, e.t_yhik
order by count desc) as tbl;

/*P-PT-INR,%,,{L-178}
INR,%,,{34714-6}
CMV IgG,ku/l,,{13949-3}
Erutrotsuutide anisotsutoos,%,,{702-1}
*/




-------------------------------------------------------
-- VALE mappingu välja võtmine
-----------------------------------------------------
-- Kui mapping on olemas nii PN -> loinc kui ka PN, PU -> loinc, siis ilmselgelt viimane on täpsem mapping ja kustutame
-- PN-> loinc ära
delete from analysiscleaning.parameter_name_to_loinc_mapping
where parameter_name in
      (
        select distinct p.parameter_name
        from analysiscleaning.parameter_name_to_loinc_mapping as p
        inner join analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping as pu
        on p.parameter_name = pu.parameter_name
      );

-- Kui mapping on olemas nii PN -> loinc kui ka PN, PU -> loinc, siis ilmselgelt viimane on täpsem mapping ja kustutame
-- PN-> loinc ära
delete from analysiscleaning.elabor_parameter_name_to_loinc_mapping
where parameter_name in
      (
        select distinct p.parameter_name
          from analysiscleaning.elabor_parameter_name_to_loinc_mapping as p
          inner join analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping as pu
          on p.parameter_name = pu.parameter_name
      );

--kontroll, see peab tühi olema
select distinct p.parameter_name
          from analysiscleaning.elabor_parameter_name_to_loinc_mapping as p
          inner join analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping as pu
          on p.parameter_name = pu.parameter_name

/*
NÄIDE
PN, loinc, PN, unit, loinc
Kolistiin,18912-6,16645-4,Kolistiin,mg/L
Moksifloksatsiin,31039-1,31039-1,Moksifloksatsiin,
Moksifloksatsiin,31039-1,43751-7,Moksifloksatsiin,mg/L
Rifampitsiin,18974-6,18974-6,Rifampitsiin,
Rifampitsiin,18974-6,4021-2,Rifampitsiin,mg/L
Vankomütsiin,19000-9,19000-9,Vankomütsiin,
Vankomütsiin,19000-9,20578-1,Vankomütsiin,mg/L
Vorikonasool,32379-0,32379-0,Vorikonasool,
Vorikonasool,32379-0,38370-3,Vorikonasool,mg/L
Porfüriinid ööpäevases uriinis,56970-7,50901-8,Porfüriinid ööpäevases uriinis,nmol/d
Porfüriinid ööpäevases uriinis,56970-7,56970-7,Porfüriinid ööpäevases uriinis,ug/d

*/


select * from classifications.elabor_analysis
where loinc_no = 'A-2912'

select * from classifications.elabor_analysis
where t_nimetus like '%Monotsüüt%'

select * from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping
where loinc_code = 'A-2912'


select * from classifications.elabor_analysis
where upper(t_lyhend) like '%P-PT%'

select * from analysiscleaning.parameter_name_to_loinc_mapping
where parameter_name like '%glukohemoglobiin%'; -- 26450-7 Eosinofiilid -> B-Eosinofiilid %


select * from analysiscleaning.elabor_parameter_name_to_loinc_mapping
where parameter_name like 'Eosinofiilid'; --26449-9, siin PN Eosinofiilid -> T_LYH B-Eosinofiilid #

select * from classifications.elabor_analysis
where kasutatav_nimetus like 'Eosinofiilid' or
      t_lyhend like 'Eosinofiilid' or
      t_nimetus like 'Eosinofiilid';

select *
from analysiscleaning.parameter_name_to_loinc_mapping
where parameter_name like 'Eosinofiilid';



--26450-7












  --leiame kohad, kus minu mapitud loinci koodi ühik ei ühti tegelikku mõõtmise ühikuga
select e.t_yhik, a.parameter_unit_raw, count(*), array_agg((a.parameter_name_raw, a.t_lyhend)) from analysiscleaning.analysis_html_loinced_runfull201904041255 as a
left join classifications.elabor_analysis as e
on a.loinc_code = e.loinc_no
where upper(a.parameter_unit_raw) != upper(e.t_yhik)
group by t_yhik, parameter_unit_raw
order by count desc;
--where upper(a.parameter_unit_raw) != upper(e.t_yhik)
--643 333 juhul ühikud ei matchi
