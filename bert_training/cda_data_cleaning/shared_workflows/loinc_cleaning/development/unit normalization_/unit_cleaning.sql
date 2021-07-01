----------------------------------------------------------------------------------------------------------------------
-- panna kõik alumine kokku
----------------------------------------------------------------------------------------------------------------------
-- kõik esinevad unitid
--POSSIBLE UNITS FAIL PEAKS OLEMA ETTE ANTUD
drop table if exists analysiscleaning.possible_units;
create table analysiscleaning.possible_units as
  select parameter_unit_raw from analysiscleaning.analysis_html_loinced_runfull201904041255
  group by parameter_unit_raw;

-- lõpliku mappingu fail
drop table if exists analysiscleaning.parameter_unit_to_loinc_unit_mapping;
create table analysiscleaning.parameter_unit_to_loinc_unit_mapping
(
  parameter_unit varchar,
  loinc_unit varchar,
  evidence varchar
);

--kõik mappimist ootavad parameter_unit_raw
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit)
select distinct parameter_unit_raw
from analysiscleaning.possible_units;
select * from analysiscleaning.parameter_unit_to_loinc_unit_mapping;

--!!!!!!!!!!!!!!!!!!!11
--puhastatud parameter_unit_raw
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit)
select distinct clean_unit from analysiscleaning.unit_cleaning as uc, analysiscleaning.possible_units as p
  where --et poleks duplikaate
        clean_unit not in (select parameter_unit from analysiscleaning.parameter_unit_to_loinc_unit_mapping) and
        upper(uc.dirty_unit) = upper(p.parameter_unit_raw);

select parameter_unit from analysiscleaning.parameter_unit_to_loinc_unit_mapping; --555
select distinct parameter_unit from analysiscleaning.parameter_unit_to_loinc_unit_mapping;--555

  /*
--puhastame uniteid

select *
from analysiscleaning.parameter_unit_to_loinc_unit_mapping
where parameter_unit like '%24%';

--lisame mappingu, kus "24h" asemel on "d"
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit, evidence)
  select regexp_replace(parameter_unit,'24h', 'd'), 'UUS'
  from analysiscleaning.parameter_unit_to_loinc_unit_mapping
  where -- et ei tekiks duplikaate
        regexp_replace(parameter_unit,'24h', 'd') not in (select parameter_unit from analysiscleaning.parameter_unit_to_loinc_unit_mapping) --and
        --regexp_replace(parameter_unit,'24h', 'd') != parameter_unit;


select *
from analysiscleaning.parameter_unit_to_loinc_unit_mapping
where parameter_unit like '%24%';


select parameter_unit, evidence from analysiscleaning.parameter_unit_to_loinc_unit_mapping; --560
select distinct parameter_unit, evidence from analysiscleaning.parameter_unit_to_loinc_unit_mapping; -- 556
--update analysiscleaning.parameter_unit_to_loinc_unit_mapping
--set parameter_unit = regexp_replace(parameter_unit,'24h', 'd');
*/
--asendame "-" -> NULL
update analysiscleaning.parameter_unit_to_loinc_unit_mapping
  set parameter_unit = null
  where parameter_unit = '-';

/*
--lisame mappingu, kus " " asemel on ""
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit)
  select regexp_replace(parameter_unit,' ', '')
  from analysiscleaning.parameter_unit_to_loinc_unit_mapping
  where regexp_replace(parameter_unit,' ', '') not in (select parameter_unit from analysiscleaning.parameter_unit_to_loinc_unit_mapping);
        --regexp_replace(parameter_unit,' ', '') != parameter_unit;

*/

----------------------------------
  -- MAPPING
-- pole veel loince külge pannud
-- nüüd eeldefineeritud reeglite järgi mapping, failist /LOINC_cleaning/development/unit normalization /unit_to_loinc_unit_mapping.txt

update analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
  set loinc_unit = l.loinc_unit, evidence = 'loinc_unit_mapping_predefined_csv'
  from analysiscleaning.loinc_unit_mapping as l
  where upper(p.parameter_unit) = upper(l.unit) or
        upper(regexp_replace(parameter_unit,'24h', 'd')) = upper(l.unit) or
        upper(regexp_replace(parameter_unit,' ', '')) = upper(l.unit);


select parameter_unit from analysiscleaning.parameter_unit_to_loinc_unit_mapping;
select distinct parameter_unit from analysiscleaning.parameter_unit_to_loinc_unit_mapping;

--PROBLEEMSED
-- nmol/d,nmol/d
--mg/d,mg/d

-------------------------------------
  -- ELABORI mapping
---- muudame " " -> ""
--insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping  (parameter_unit)
--  (select regexp_replace(parameter_unit,' ', '') from analysiscleaning.parameter_unit_to_loinc_unit_mapping);


update analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
set loinc_unit = e.t_yhik, evidence = 'parameter_unit_to_elabor_tyhik'
from classifications.elabor_analysis as e
where upper(parameter_unit) = upper(t_yhik) or
        upper(regexp_replace(parameter_unit,'24h', 'd')) = upper(t_yhik) or
        upper(regexp_replace(parameter_unit,' ', '')) = upper(t_yhik);;

--null mappingul pole mõtet
delete from analysiscleaning.parameter_unit_to_loinc_unit_mapping
where loinc_unit is null;

select parameter_unit, count(*) from analysiscleaning.parameter_unit_to_loinc_unit_mapping
group by parameter_unit;

select count(*) from analysiscleaning.parameter_unit_to_loinc_unit_mapping
--  where loinc_unit is not null;


---------------------------------------
--paljudele ridadest saab unit
 select  count(*) from
--select count(*) from
  -- mappingu rakendamine andmetele
  (
      select p.loinc_unit, a.parameter_unit_raw, p.evidence, a.*
      from analysiscleaning.analysis_html_loinced_runfull201904041255 as a
        left join analysiscleaning.parameter_unit_to_loinc_unit_mapping as p on
        (a.parameter_unit_raw) = (p.parameter_unit)
  ) as tbl
where loinc_unit is not null and parameter_unit is not null;
--group by parameter_unit
--order by count desc;


-- loinc unit on olemas 2400215  --2412672 real
 -- loinc unit on puudu 3116331 real, 1908567 neist üleüldse puudu parameter_unit
  -- ridu millel ÜLDSE on olemas parameter_unit_raw 3620436
  --seega mappimata parameter_uniteid on 1207764
  -- mapitud on 2412672/3620436  = 66%

select * from classifications.elabor_analysis
where t_yhik = 'mmol/L';

select *
from analysiscleaning.parameter_unit_to_loinc_unit_mapping
where parameter_unit = 'mmol/l';

/*
mmol/l,266063
mmol/L,183581
g/l,146374
fL,114351
U/L,95205
*/

select count(*) from analysiscleaning.analysis_html_loinced_runfull201904041255;



select count(*)
from analysiscleaning.analysis_html_loinced_runfull201904041255;







----------------------------------------------------------------------------------------------------------------------
-- kõik tore ja toimib
----------------------------------------------------------------------------------------------------------------------
-- erinevad esinevate ühikute tabel
drop table if exists analysiscleaning.possible_units;
create table analysiscleaning.possible_units as
  select parameter_unit_raw, count(*) from analysiscleaning.analysis_html_loinced_runfull201904041255
  group by parameter_unit_raw
  order by count desc;

alter table analysiscleaning.possible_units
  add column cleaned_unit varchar,
  add column loinc_unit varchar,
  add column evidence varchar;

-- unit_cleaning on saadud failist cda-data-cleaning--  analysis_data_cleaning--  value_cleaning--  data--  unit_mapping.txt
update analysiscleaning.possible_units as p
    set cleaned_unit = u.clean_unit
    from analysiscleaning.unit_cleaning as u
    where upper(p.parameter_unit_raw) = upper(u.dirty_unit);

-- table unit to unit_to_loinc_unit_mapping HALB NIMI
-- leitud  analysis_data_cleaning/name_cleaning/clean_values.py
-- dict_special_units = {'sek': 's', 'ug/ml': 'mg/l', 'mg/mmol': 'g/mol',
--                           'mg/lfeu': 'mg/l', '%;mmol/mol': '%', '10e6/l': 'e6/l', '10e9/l': 'e9/l',
--                           '10e12/l': 'e12/l', '10*9/l': 'e9/l', '/pl': 'e12/l', 'pg/ml': 'ng/l',
--                           'suhtarv': '%', 'µg/ml': 'mg/l', 'u/ml': 'ku/l',
--                           'ng/ml': 'ug/l', '/ul': 'e6/l', 'iu/ml': 'kiu/l', 'ng/ml': 'ug/l', '/nl':'e9/l'}

--clean_unitis asendame 24h -> d ja
update analysiscleaning.possible_units
  set cleaned_unit = regexp_replace(cleaned_unit,'24h', 'd');

--asendame "-" -> NULL
update analysiscleaning.possible_units
  set parameter_unit_raw = null
  where parameter_unit_raw = '-';


update analysiscleaning.possible_units as p
  set loinc_unit = l.loinc_unit, evidence = 'loinc_unit_mapping_predefined_csv'
  from analysiscleaning.loinc_unit_mapping as l
  where upper(p.cleaned_unit) = upper(l.unit) or
        upper(p.parameter_unit_raw) = upper(l.unit);

--mitte null uniteid - 3092933
select sum(count) from analysiscleaning.possible_units
where parameter_unit_raw is not null;

-- palju unititest sai mapitud
--464 982/3 092 933
select sum(count) from analysiscleaning.possible_units
where loinc_unit is not null;

select parameter_unit_raw, count from analysiscleaning.possible_units
where loinc_unit is null
order by count desc;



------------------------------------------------------------------------------------------
-- ELABORI järgi
------------------------------------------------------------------------------------------

--clean_unitis asendame " " -> ""
update analysiscleaning.possible_units
  set cleaned_unit = regexp_replace(cleaned_unit,' ', '');

-- mitmed parameter_unit_raw ja cleaned_unitid on juba õigel kujul
update analysiscleaning.possible_units as p
set loinc_unit = e.t_yhik, evidence = 'parameter_unit_raw_to_elabor_tyhik'
from classifications.elabor_analysis as e
where upper(parameter_unit_raw) = upper(t_yhik);

update analysiscleaning.possible_units as p
set loinc_unit = e.t_yhik, evidence = 'cleaned_unit_raw_to_elabor_tyhik'
from classifications.elabor_analysis as e
where p.loinc_unit is null and
      upper(p.cleaned_unit) = upper(e.t_yhik);


-- palju unititest sai mapitud
-- 2 771 305 / 3 092 933 = 89%
select sum(count) from analysiscleaning.possible_units
where loinc_unit is not null;

select parameter_unit_raw, count from analysiscleaning.possible_units
where loinc_unit is null
order by count desc;



--------------------------------------------------------------------------------------------------------
  --eeldefineeritud failidest mapping
--------------------------------------------------------------------------------------------------------
drop table if exists analysiscleaning.parameter_unit_to_loinc_unit_mapping;
create table analysiscleaning.parameter_unit_to_loinc_unit_mapping(
  parameter_unit varchar,
  loinc_unit varchar,
  evidence varchar);


-- lisame mappingu kus on dirty_parameter_unit -> t_yhik
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit, loinc_unit, evidence)
    select distinct p.parameter_unit_raw, p.loinc_unit, 'dirty_parameter_unit'
    from  analysiscleaning.possible_units as p
    where p.parameter_unit_raw is not null and
          p.loinc_unit is not null;

select * from analysiscleaning.parameter_unit_to_loinc_unit_mapping
  where evidence =  'dirty_parameter_unit' and
        upper(parameter_unit) != upper(loinc_unit);

-- lisame  mappingu kus on clean_parameter_unit -> t_yhik
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit, loinc_unit, evidence)
    select distinct p.cleaned_unit, p.loinc_unit, 'clean_parameter_unit'
    from analysiscleaning.possible_units as p
    where p.cleaned_unit is not null and
          p.loinc_unit is not null;


-----------------------------------------------------------------------------------------------------
-- teen nüüd neist elabori omadest ka csv failid
-------------------------------------------------------------------------------------------------


-- lisame ELABORI mappingu t_yhik -> t_yhik
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit, loinc_unit, evidence)
    select distinct e.t_yhik, e.t_yhik, 'elabor_direct'
    from classifications.elabor_analysis as e
    where t_yhik is not null;

insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping(parameter_unit, loinc_unit, evidence)
    select distinct p.parameter_unit_raw, e.t_yhik, 'elabor_dirty_code'
    from classifications.elabor_analysis as e, analysiscleaning.possible_units as p
    where upper(parameter_unit_raw) = upper(t_yhik);


insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping(parameter_unit, loinc_unit, evidence)
    select distinct p.cleaned_unit, e.t_yhik, 'elabor_clean_code'
    from classifications.elabor_analysis as e, analysiscleaning.possible_units as p
    where upper(p.cleaned_unit) = upper(t_yhik);








select * from analysiscleaning.parameter_unit_to_loinc_unit_mapping
  where evidence =  'clean_parameter_unit'; and
        upper(parameter_unit) != upper(loinc_unit);

select * from analysiscleaning.parameter_unit_to_loinc_unit_mapping
where parameter_unit like '%10^12/l%';




select * from analysiscleaning.parameter_unit_to_loinc_unit_mapping;

select distinct t_yhik
from classifications.elabor_analysis
where t_yhik is not null;

--select t_yhik from classifications.elabor_analysis


















-----------------------------------------------------------------------------------------------------
--leitud  analysis_data_cleaning/name_cleaning/clean_values.py
;dict_special_units = {'sek': 's', 'ug/ml': 'mg/l', 'mg/mmol': 'g/mol',
                          'mg/lfeu': 'mg/l', '%;mmol/mol': '%', '10e6/l': 'e6/l', '10e9/l': 'e9/l',
                          '10e12/l': 'e12/l', '10*9/l': 'e9/l', '/pl': 'e12/l', 'pg/ml': 'ng/l',
                          'suhtarv': '%', 'µg/ml': 'mg/l', 'u/ml': 'ku/l',
                          'ng/ml': 'ug/l', '/ul': 'e6/l', 'iu/ml': 'kiu/l', 'ng/ml': 'ug/l', '/nl':'e9/l'}

select t_lyhend, t_nimetus, loinc_no from classifications.elabor_analysis
where t_lyhend like '%Baso%';

select * from work.runfull201903141705_analysis_html
limit 1;

-- loome uue veeru, mida hakkame puhastama
alter table analysiscleaning.analysis_html_loinced_runfull201904041255
  add column parameter_unit varchar;
update analysiscleaning.analysis_html_loinced_runfull201904041255
  set parameter_unit = parameter_unit_raw;


update analysiscleaning.analysis_html_loinced_runfull201904041255 as a
set parameter_unit = c.column_a
from (values
    ('sek', 's'),
    ('suhtarv', '%'),
    ('ug/ml': 'mg/l'),
    ('mg/mmol': 'g/mol'),
    ('mg/lfeu': 'mg/l'),
    ('%;mmol/mol': '%'),
    ('10e6/l': 'e6/l'),
    ('10e9/l': 'e9/l'),
    ('10e12/l': 'e12/l'),
    ('10*9/l': 'e9/l'),
    ('/pl': 'e12/l'),
    ('pg/ml': 'ng/l'),
    ('suhtarv': '%'),
    ('µg/ml': 'mg/l'), ('u/ml': 'ku/l',
                          'ng/ml': 'ug/l', '/ul': 'e6/l', 'iu/ml': 'kiu/l', 'ng/ml': 'ug/l', '/nl':'e9/l'

) as c(column_b, column_a)
where c.column_b = a.parameter_unit;


select * from analysiscleaning.analysis_html_loinced_runfull201904041255
where parameter_unit_raw != parameter_unit;

select distinct parameter_unit_raw, parameter_unit from analysiscleaning.analysis_html_loinced_runfull201904041255
where parameter_unit_raw like 's%';