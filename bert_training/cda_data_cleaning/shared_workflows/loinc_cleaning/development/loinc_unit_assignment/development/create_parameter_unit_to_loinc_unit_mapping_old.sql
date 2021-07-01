-- kõik parameter_unitid, mida vaja mappida loinc_unitiks peavad olema antud eraldi .csv failina "data/possible_units.csv"
drop table if exists analysiscleaning.temp_possible_units;
create table analysiscleaning.temp_possible_units
(
	parameter_unit_raw text
);
\copy analysiscleaning.temp_possible_units from 'data/possible_units.csv' with (delimiter ',', format csv);


-- unitite cleanimise tabel (eeldefineeritud reeglitest) failist "data/unit_cleaning.csv"
drop table if exists analysiscleaning.temp_unit_cleaning;
create table analysiscleaning.temp_unit_cleaning(
  dirty_unit text,
  clean_unit text
);
\copy analysiscleaning.temp_unit_cleaning from 'data/unit_cleaning.csv' with (delimiter ',', format csv, header);


-- parameter_unitite mappimine loinc_unititkes (eeldefineeritud reeglitest) failist "data/unit_to_loinc_unit_mapping.csv"
drop table if exists analysiscleaning.temp_unit_to_loinc_unit_mapping;
create table analysiscleaning.temp_unit_to_loinc_unit_mapping(
  parameter_unit text,
  loinc_unit text
);
\copy analysiscleaning.temp_unit_to_loinc_unit_mapping from 'data/unit_to_loinc_unit_mapping.csv' with (delimiter ',', format csv, header);

-- lõpliku mappingu tabel
drop table if exists analysiscleaning.parameter_unit_to_loinc_unit_mapping;
create table analysiscleaning.parameter_unit_to_loinc_unit_mapping
(
  parameter_unit varchar,
  parameter_unit_clean varchar,
  loinc_unit varchar,
  evidence varchar
);


--kõik mappimist ootavad parameter_united (possible_unit'st)
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit)
    select distinct parameter_unit_raw
    from analysiscleaning.temp_possible_units;


-------------------------------------------------------------------------------------
-- CLEANING
-------------------------------------------------------------------------------------


-- puhastame parameter_unitid (lähtume failist unit_cleaning.csv)
update analysiscleaning.parameter_unit_to_loinc_unit_mapping
set parameter_unit_clean = cleaned.clean_unit
from (select uc.clean_unit, uc.dirty_unit from analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
    left join analysiscleaning.temp_unit_cleaning as uc
    on p.parameter_unit = uc.dirty_unit) as cleaned
where parameter_unit = cleaned.dirty_unit;

-- kui cleanitud veerus jääb lahter tühjaks -> paneme sinna lihtsalt tavalise (mitte cleanitud) parameter uniti
update analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
set parameter_unit_clean = parameter_unit
where p.parameter_unit_clean is NULL;


-- asendame 24h -> d
update analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
  set parameter_unit_clean = (regexp_replace(p.parameter_unit_clean,'24h', 'd'));

update analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
  set parameter_unit_clean = (regexp_replace(p.parameter_unit_clean,'24H', 'd'));

-- asendame " " -> ""
update analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
  set parameter_unit_clean = (regexp_replace(p.parameter_unit_clean,' ', ''));

----------------------------------------------------------------------------------------
  -- MAPPING
----------------------------------------------------------------------------------------

-- LOINCi uniti külge panemine (lähtub eeldefineeritud faili loinc_unit_mapping.csv)
update analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
  set loinc_unit = upper(l.loinc_unit), evidence = 'manual_mapping'
  from analysiscleaning.temp_unit_to_loinc_unit_mapping as l
  where upper(p.parameter_unit) = upper(l.parameter_unit) or
        upper(p.parameter_unit_clean) = upper(l.parameter_unit);


-- LOINCI unit ELABORI põhjal (elaboris kutsutakse seda 't_yhik'-ks)
update analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
set loinc_unit = e.t_yhik, evidence = 'elabor_tyhik'
from classifications.elabor_analysis as e
where upper(p.parameter_unit) = upper(e.t_yhik) or
        upper(p.parameter_unit_clean) = upper(e.t_yhik);

-- parameter_unit -> clean_unit -> NULL
-- selline juht, kus küll puhastame uniti ära, aga loinci unitit ikka ei oska külge panna
-- teeme neist eraldi tabeli
\copy (select distinct * from analysiscleaning.parameter_unit_to_loinc_unit_mapping where loinc_unit is null) to 'mapping_result/parameter_unit_to_cleaned_unit.csv' With (delimiter ',', format csv, header);

-- nüüd võib parameter_unit -> NULL ära kustutada mappingu tabelist
delete from analysiscleaning.parameter_unit_to_loinc_unit_mapping
where loinc_unit is null;

-- "-" -> NULL loomine
insert into analysiscleaning.parameter_unit_to_loinc_unit_mapping (parameter_unit, loinc_unit, evidence)
values ('-', NULL, 'manual_mapping');

-- temporary tabeleid pole enam vaja
--drop table analysiscleaning.temp_possible_units;
--drop table analysiscleaning.temp_unit_cleaning;
--drop table analysiscleaning.temp_unit_to_loinc_unit_mapping;

-- lõpliku mappingu tabel "parameter_unit_to_loinc_unit_mapping" csv faili
\copy (select distinct * from analysiscleaning.parameter_unit_to_loinc_unit_mapping) to 'mapping_result/parameter_unit_to_loinc_mapping.csv' With (delimiter ',', format csv, header);
