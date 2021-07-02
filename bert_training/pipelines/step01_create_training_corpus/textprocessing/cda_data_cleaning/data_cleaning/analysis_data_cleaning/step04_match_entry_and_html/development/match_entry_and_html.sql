-- TABELID MIDA MATCHIDA - run_201909111308_analysis_entry_loinced_unique ja run_201909111308_analysis_html_loinced_unique

set search_path to work;

---------------------------------------------------------------------------------------------------------------
-- IDEAALSED EPIKRIISID - ridu 8178
---------------------------------------------------------------------------------------------------------------
-- kõik epi_id, mis esinevad NII HTML KUI KA ENTRYS
-- ja mille ridade arv html ja entrys on võrdne
create table run_201908261300_ideal_duplicate_epi_id as
with html_epi_count as
         (select epi_id as html_epi_id, count(*) as html_count
          from work.run_201908261300_analysis_html_loinced
          group by epi_id),
     entry_epi_count as
         (select epi_id as entry_epi_id, count(*) as entry_count
          from work.run_201908261300_analysis_entry_loinced
          group by epi_id)
select html_epi_id
from html_epi_count --select html_epi_id, entry_epi_id, html_count, entry_count
         inner join entry_epi_count
                    on html_epi_id = entry_epi_id
where entry_count = html_count;


---------------------------------------------------------------------------------------------------------------
-- IDEAALSED EPIKRIISID 2
---------------------------------------------------------------------------------------------------------------
-- epikriis esineb AINULT HTMLs või AINULT ENTRYS aga mitte mõlemas
-- full outer join - only rows unique to both tables
create table run_201908261300_ideal_unique_epi_id as
select distinct html.epi_id
from run_201908261300_analysis_html_loinced as html
         full outer join
         (select epi_id from run_201908261300_analysis_entry_loinced) as entry
         on html.epi_id = entry.epi_id;

------------------------------------------------------------------------------------------------------------------------
-- Mingid unresolved tabelid
------------------------------------------------------------------------------------------------------------------------
ALTER TABLE run_201909021100_analysis_entry_loinced_unique
    ADD source varchar NOT NULL DEFAULT 'entry';

ALTER TABLE run_201909021100_analysis_html_loinced_unique
    ADD source varchar NOT NULL DEFAULT 'html';

ALTER TABLE run_201909021100_analysis_entry_loinced_unique
    drop column rn;

ALTER TABLE run_201909021100_analysis_html_loinced_unique
    drop column rn;

/*
-- kõik html'i epi_id mis on html-s ja samas ka entrys
create table run_201908261300_unresolved_html as
with unique_in_html as (
    -- kõik epi_id, mis on UNIKAALSED ehk ainult html tabelis
    SELECT distinct html.epi_id
    FROM   run_201908261300_analysis_html_loinced as html
            LEFT JOIN run_201908261300_analysis_entry_loinced as entry
                on html.epi_id = entry.epi_id
    WHERE  entry.epi_id IS NULL)
select distinct epi_id from run_201908261300_analysis_html_loinced
    where epi_id not in (select epi_id from unique_in_html);

select * from run_201908261300_analysis_html_loinced where epi_id = '16780486';
select * from  run_201908261300_analysis_entry_loinced where epi_id = '16780486';

-- kõik entry epi_id mis on entry-s ja samas ka html's
create table run_201908261300_unresolved_entry as
with unique_in_entry as (
    -- kõik epi_id, mis on UNIKAALSED ehk ainult entry tabelis
    SELECT distinct entry.epi_id
    FROM run_201908261300_analysis_entry_loinced as entry
             LEFT JOIN run_201908261300_analysis_html_loinced as html
                       on html.epi_id = entry.epi_id
    WHERE html.epi_id IS NULL
)
select distinct epi_id from run_201908261300_analysis_entry_loinced
    where epi_id not in (select epi_id from unique_in_entry);
*/
-- unique in entry
/*SELECT distinct entry.epi_id, entry.id
    FROM run_201908261300_analysis_entry_loinced as entry
             LEFT JOIN run_201908261300_analysis_html_loinced as html
                       on html.epi_id = entry.epi_id;
4644430
6627803
52908975
16036176
17033677
37826116
16036176

42475
62770
55296
25297
28541
56084
25996
*/
/*
-- kõik epi_id, mis on UNIKAALSED ehk ainult html tabelis
SELECT distinct html.id, html.epi_id
    FROM   run_201908261300_analysis_html_loinced as html
            LEFT JOIN run_201908261300_analysis_entry_loinced as entry
                on html.epi_id = entry.epi_id
    WHERE  entry.epi_id IS NULL;

51155312
37316924
52094355
21423890
31152523
13903789

5114779
3987870
3057929
1283165
1762900
453026

*/


-- kõik html'i read (id), kui epi_id esineb nii entrys kui htmls
-- ülejäänud unikaalsed
drop table if exists run_201909021100_unresolved_html;
create table run_201909021100_unresolved_html as
select id
from run_201909021100_analysis_html_loinced_unique as html
where html.epi_id in (select epi_id from run_201909021100_analysis_entry_loinced_unique);

--ridu 70193, 67128, 63961, 61656
select count(*)
from run_201909021100_unresolved_html;

-- kõik entry read (id), kui epi_id esineb nii entrys kui htmls
drop table if exists run_201909021100_unresolved_entry;
create table run_201909021100_unresolved_entry as
select id
from run_201909021100_analysis_entry_loinced_unique as entry
where entry.epi_id in (select epi_id from run_201909021100_analysis_html_loinced_unique);

-- 68783, 68783, 61730,61730
select count(*)
from run_201909021100_unresolved_entry;



----------------------------------------------------------------------------
--1. perfektse matchingu tabel PEAKS LISAMA KA HMTL UNIQUE JA ENTRY UNIQUE
------------------------------------------------------------------------------
--  html.epi_id = entry.epi_id and
--  html.analysis_name = entry.analysis_name and
--  html.parameter_name = entry.parameter_name and
--  html.parameter_unit = entry.parameter_unit and
--  html.effective_time = entry.effective_time and
--  html.value = entry.value and
--  html.reference_values = entry.reference_values;

drop table if exists run_201909021100_matched;
create table run_201909021100_matched as
select html.id                                                                                  as id_html,
       entry.id                                                                                 as id_entry,
       html.row_nr                                                                              as row_nr_html,
       html.epi_id                                                                              as epi_id_html,
       html.epi_type                                                                            as epi_type_html,
       html.parse_type                                                                          as parse_type_html,
       html.panel_id                                                                            as panel_id_html,
       html.analysis_name_raw                                                                   as analysis_name_raw_html,
       html.parameter_name_raw                                                                  as parameter_name_raw_html,
       html.parameter_unit_raw                                                                  as parameter_unit_raw_html,
       html.reference_values_raw                                                                as reference_values_raw_html,
       html.value_raw                                                                           as value_raw_html,
       html.analysis_substrate_raw                                                              as analysis_substrate_raw_html,
       html.analysis_substrate                                                                  as analysis_substrate_html,
       html.entry_match_id                                                                      as entry_match_id_html,
       html.entry_html_match_desc                                                               as entry_html_match_desc_html,
       html.effective_time_raw                                                                  as effective_time_raw_html,
       html.cleaned_value                                                                       as cleaned_value_html,
       html.analysis_name                                                                       as analysis_name_html,
       html.parameter_name                                                                      as parameter_name_html,
       html.effective_time                                                                      as effective_time_html,
       html.value                                                                               as value_html,
       html.parameter_unit_from_suffix                                                          as parameter_unit_from_suffix_html,
       html.suffix                                                                              as suffix_html,
       html.value_type                                                                          as value_type_html,
       html.reference_values                                                                    as reference_values_html,
       html.parameter_unit                                                                      as parameter_unit_html,
       html.loinc_unit                                                                          as loinc_unit_html,
       html.elabor_t_lyhend                                                                     as elabor_t_lyhend_html,
       html.loinc_code                                                                          as loinc_code_html,
       html.source                                                                              as source_html,
       entry.original_analysis_entry_id,
       entry.epi_id                                                                             as epi_id_entry,
       entry.epi_type                                                                           as epi_type_entry,
       entry.analysis_id                                                                        as analysis_id_entry,
       entry.code_system                                                                        as code_system_entry,
       entry.code_system_name                                                                   as code_system_name_entry,
       entry.analysis_code_raw                                                                  as analysis_code_raw_entry,
       entry.analysis_name_raw                                                                  as analysis_name_raw_entry,
       entry.parameter_code_raw                                                                 as parameter_code_raw_entry,
       entry.parameter_name_raw                                                                 as parameter_name_raw_entry,
       entry.parameter_unit_raw                                                                 as parameter_unit_raw_entry,
       entry.reference_values_raw                                                               as reference_values_raw_entry,
       entry.effective_time_raw                                                                 as effective_time_raw_entry,
       entry.value_raw                                                                          as value_raw_entry,
       entry.cleaned_value                                                                      as cleaned_value_entry,
       entry.analysis_name                                                                      as analysis_name_entry,
       entry.parameter_name                                                                     as parameter_name_entry,
       entry.effective_time                                                                     as effective_time_entry,
       entry.value                                                                              as value_entry,
       entry.parameter_unit_from_suffix                                                         as parameter_unit_from_suffix_entry,
       entry.suffix                                                                             as suffix_entry,
       entry.value_type                                                                         as value_type_entry,
       entry.reference_values                                                                   as reference_values_entry,
       entry.parameter_unit                                                                     as parameter_unit_entry,
       entry.loinc_unit                                                                         as loinc_unit_entry,
       entry.elabor_t_lyhend                                                                    as elabor_t_lyhend_entry,
       entry.loinc_code                                                                         as loinc_code_entry,
       entry.source                                                                             as source_entry,
       'analysis_name, parameter_name, parameter_unit, effective_time, value, reference_values' as match_description
from run_201909021100_analysis_html_loinced_unique as html
         join run_201909021100_analysis_entry_loinced_unique as entry
              on html.epi_id = entry.epi_id and
                 html.analysis_name = entry.analysis_name and
                 html.parameter_name = entry.parameter_name and
                 html.parameter_unit = entry.parameter_unit and
                 html.effective_time = entry.effective_time and
                 html.value = entry.value and
                 html.reference_values = entry.reference_values;

--6103,6081, 5953
select count(*)
from run_201909021100_matched;

----------------------------------------------------------------------------------
-- 1.2 epikriis esineb AINULT HTMLs või AINULT ENTRYS aga mitte mõlemas
-- Põhimõtteliselt ideaalsed
-- full outer join - only rows unique to both tables
----------------------------------------------------------------------------------

with unique_in_entry as (
-- kõik epi_id, mis on UNIKAALSED ehk ainult ENTRY tabelis
    SELECT distinct entry.epi_id
    FROM run_201909021100_analysis_entry_loinced_unique as entry
             LEFT JOIN run_201909021100_analysis_html_loinced_unique as html
                       on html.epi_id = entry.epi_id
    WHERE html.epi_id IS NULL
)
insert
into run_201909021100_matched (id_entry,
                               original_analysis_entry_id,
                               epi_id_entry,
                               epi_type_entry,
                               analysis_id_entry,
                               code_system_entry,
                               code_system_name_entry,
                               analysis_code_raw_entry,
                               analysis_name_raw_entry,
                               parameter_code_raw_entry,
                               parameter_name_raw_entry,
                               parameter_unit_raw_entry,
                               reference_values_raw_entry,
                               effective_time_raw_entry,
                               value_raw_entry,
                               cleaned_value_entry,
                               analysis_name_entry,
                               parameter_name_entry,
                               effective_time_entry,
                               value_entry,
                               parameter_unit_from_suffix_entry,
                               suffix_entry,
                               value_type_entry,
                               reference_values_entry,
                               parameter_unit_entry,
                               loinc_unit_entry,
                               elabor_t_lyhend_entry,
                               loinc_code_entry,
                               source_entry,
                               match_description)
    (select *, 'only entry' as match_description
     from run_201909021100_analysis_entry_loinced_unique
     where epi_id in (select * from unique_in_entry));


-----------------------------------------------------
with unique_in_html as (
    -- kõik epi_id, mis on UNIKAALSED ehk ainult HTML tabelis
    -- 95 828 unikaalset epi_id't htmls
    -- 4 466 004 unikaalse epi'ga RIDA htmls
    SELECT html.epi_id
    FROM run_201909021100_analysis_html_loinced_unique as html
             LEFT JOIN run_201909021100_analysis_entry_loinced_unique as entry
                       on html.epi_id = entry.epi_id
    WHERE entry.epi_id IS NULL
)
insert
into run_201909021100_matched (row_nr_html,
                               epi_id_html,
                               epi_type_html,
                               parse_type_html,
                               panel_id_html,
                               analysis_name_raw_html,
                               parameter_name_raw_html,
                               parameter_unit_raw_html,
                               reference_values_raw_html,
                               value_raw_html,
                               analysis_substrate_raw_html,
                               analysis_substrate_html,
                               entry_match_id_html,
                               entry_html_match_desc_html,
                               id_html,
                               effective_time_raw_html,
                               cleaned_value_html,
                               analysis_name_html,
                               parameter_name_html,
                               effective_time_html,
                               value_html,
                               parameter_unit_from_suffix_html,
                               suffix_html,
                               value_type_html,
                               reference_values_html,
                               parameter_unit_html,
                               loinc_unit_html,
                               elabor_t_lyhend_html,
                               loinc_code_html,
                               source_html,
                               match_description)
    (select *, 'only_html'
     from run_201909021100_analysis_html_loinced_unique
     where epi_id in (select * from unique_in_html));

-- algselt ridu
select count(*)
from run_201909021100_analysis_html_loinced; --4536197,4306542, ,4338914
select count(*)
from run_201909021100_analysis_entry_loinced;  --69033,69033

-- nüüdseks "matchitud"
select count(*)
from run_201909021100_matched;
--4 472 357, 4245745, 4141237, 3968059
-- matchiMATA 132 873


----------------------------------------------------------------------------------
-- 2. UNRESOLVE VÄHENDAMINE
-- eemaldame unresolvedes resolved_read = perfektsed_matchingud
----------------------------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_entry; --68783, 61730

DELETE
FROM run_201909021100_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909021100_matched);

select count(*)
from run_201909021100_unresolved_entry; --62748, 55777
---------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_html; --70193,67128,63961, 61656

DELETE
FROM run_201909021100_unresolved_html
WHERE id in (SELECT id_html FROM run_201909021100_matched);

select count(id)
from run_201909021100_unresolved_html; --64207,61153,58009, 55704

--pole duplikaate
select id, count(*)
from run_201909021100_unresolved_html
group by id
having count(*) > 2;

--------------------------------------------------------------
-- NB!!!!!  LOINCIMINE TEKITAB DUPLIKAATE
--------------------------------------------------------------
--id = 873352
--id = 911147
--id = 795357
--id = 362986

-- Puhastamise veel pole duplikaate
select *
from run_201908261300_analysis_html_cleaned
where id = 795357;
select count(*)
from run_201908261300_analysis_html_cleaned;
--4246711

-- LOINCimisel tekivad duplikaadid
select *
from run_201908261300_analysis_html_loinced
where id = 795357;
select count(*)
from run_201908261300_analysis_html_loinced;
--4536197 ehk 289 486 rida tekib juurde!!!


----------------------------------------------------------------------------------
-- 3.1 OPTIMISTLIK MATCHIMINE  V E D PN
----------------------------------------------------------------------------------
-- allesjäänud unresolved id-ga read
-- ühendan epi_id, value, effective_time järgi
--V = value E = epi_id D = effective_time PN = parameter_name

--5264, aga siin võib olla valesti ühendamisi
drop table if exists run_201909021100_optimistic_matching_VEDPN;
create table run_201909021100_optimistic_matching_VEDPN as
select html.id                          as id_html,
       entry.id                         as id_entry,
       html.row_nr                      as row_nr_html,
       html.epi_id                      as epi_id_html,
       html.epi_type                    as epi_type_html,
       html.parse_type                  as parse_type_html,
       html.panel_id                    as panel_id_html,
       html.analysis_name_raw           as analysis_name_raw_html,
       html.parameter_name_raw          as parameter_name_raw_html,
       html.parameter_unit_raw          as parameter_unit_raw_html,
       html.reference_values_raw        as reference_values_raw_html,
       html.value_raw                   as value_raw_html,
       html.analysis_substrate_raw      as analysis_substrate_raw_html,
       html.analysis_substrate          as analysis_substrate_html,
       html.entry_match_id              as entry_match_id_html,
       html.entry_html_match_desc       as entry_html_match_desc_html,
       html.effective_time_raw          as effective_time_raw_html,
       html.cleaned_value               as cleaned_value_html,
       html.analysis_name               as analysis_name_html,
       html.parameter_name              as parameter_name_html,
       html.effective_time              as effective_time_html,
       html.value                       as value_html,
       html.parameter_unit_from_suffix  as parameter_unit_from_suffix_html,
       html.suffix                      as suffix_html,
       html.value_type                  as value_type_html,
       html.reference_values            as reference_values_html,
       html.parameter_unit              as parameter_unit_html,
       html.loinc_unit                  as loinc_unit_html,
       html.elabor_t_lyhend             as elabor_t_lyhend_html,
       html.loinc_code                  as loinc_code_html,
       html.source                      as source_html,
       entry.original_analysis_entry_id,
       entry.epi_id                     as epi_id_entry,
       entry.epi_type                   as epi_type_entry,
       entry.analysis_id                as analysis_id_entry,
       entry.code_system                as code_system_entry,
       entry.code_system_name           as code_system_name_entry,
       entry.analysis_code_raw          as analysis_code_raw_entry,
       entry.analysis_name_raw          as analysis_name_raw_entry,
       entry.parameter_code_raw         as parameter_code_raw_entry,
       entry.parameter_name_raw         as parameter_name_raw_entry,
       entry.parameter_unit_raw         as parameter_unit_raw_entry,
       entry.reference_values_raw       as reference_values_raw_entry,
       entry.effective_time_raw         as effective_time_raw_entry,
       entry.value_raw                  as value_raw_entry,
       entry.cleaned_value              as cleaned_value_entry,
       entry.analysis_name              as analysis_name_entry,
       entry.parameter_name             as parameter_name_entry,
       entry.effective_time             as effective_time_entry,
       entry.value                      as value_entry,
       entry.parameter_unit_from_suffix as parameter_unit_from_suffix_entry,
       entry.suffix                     as suffix_entry,
       entry.value_type                 as value_type_entry,
       entry.reference_values           as reference_values_entry,
       entry.parameter_unit             as parameter_unit_entry,
       entry.loinc_unit                 as loinc_unit_entry,
       entry.elabor_t_lyhend            as elabor_t_lyhend_entry,
       entry.loinc_code                 as loinc_code_entry,
       entry.source                     as source_entry
from run_201909021100_analysis_html_loinced_unique as html
         join run_201909021100_analysis_entry_loinced_unique as entry
              on html.epi_id = entry.epi_id and
                  --html.analysis_name = entry.analysis_name and
                 html.parameter_name = entry.parameter_name and
                  --html.parameter_unit = entry.parameter_unit and
                 date(html.effective_time) = date(entry.effective_time) and
                 html.value = entry.value
     --html.reference_values = entry.reference_values;
where html.id in (select id from run_201909021100_unresolved_html)
   or entry.id in (select id from run_201909021100_unresolved_entry);

--40958, 38228, 32072, 31946
select count(*)
from run_201909021100_optimistic_matching_VEDPN;

/* loinci erinevus...
select elabor_t_lyhend_entry, elabor_t_lyhend_html, parameter_name_entry, parameter_name_html, parameter_unit_entry, parameter_unit_html, analysis_name_entry, analysis_name_html
from run_201908261300_optimistic_matching_VEDPN
where elabor_t_lyhend_entry != elabor_t_lyhend_html;
*/

----------------------------------------------------------------------------------
-- 3.2 OPTIMISTLIK MATCHIMINE  V E D PN ! UNIT !
----------------------------------------------------------------------------------
--V = value E = epi_id D = effective_time PN = parameter_name U = parameter_unit
-- 29386 rida,
drop table if exists run_201909021100_optimistic_matching_VEDPNU;
create table run_201909021100_optimistic_matching_VEDPNU as
select *
from run_201909021100_optimistic_matching_VEDPN
where epi_id_html = epi_id_entry
  and value_html = value_entry
  and date(effective_time_html) = date(effective_time_entry)
  and parameter_name_html = parameter_name_entry
  --unitite erinvad cased
  and ((parameter_unit_html is not distinct from parameter_unit_entry) or --(et NULL = NULL oleks TRUE)
    (parameter_unit_entry is NULL and parameter_unit_html is NOT NULL) or --ühes tabelis unit puudu, teises olemas
    (parameter_unit_html is NULL and parameter_unit_entry is NOT NULL));
--29386, 26978, 24108, 31418, 31946
select count(*)
from run_201909021100_optimistic_matching_VEDPNU;

---lisan matched tabelisse UNITI järgi matchitud
-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!! TEGELT KOMMENTEERI SISSE
insert into run_201909021100_matched
    (select *, 'epi_id, parameter_name, parameter_unit, effective_time, value'
     from run_201909021100_optimistic_matching_VEDPNU);




     /*where epi_id_html = epi_id_entry
       and value_html = value_entry
       and date(effective_time_html) = date(effective_time_entry)
       and parameter_name_html = parameter_name_entry
       and parameter_unit_html = parameter_unit_entry);*/

--select * from run_201908261300_optimistic_matching_VEDPNU limit 10;
/* uniti järgi matchitud
2367878
5604
833603
1292277
2199540
2584767
*/
select count(*) from run_201909021100_optimistic_matching_VEDPN; --32072,31946

-- eemaldan nüüd VEDPN tabelist read, mis said matchitud UNITi abil (VEDPNU)
/*delete
from run_201909021100_optimistic_matching_VEDPN as vedpn
    using run_201909021100_optimistic_matching_VEDPNU as vedpnu
where vedpn.id_entry = vedpnu.id_entry;*/



/*select *
from run_201909021100_optimistic_matching_VEDPN
    except
select *
from run_201909021100_optimistic_matching_VEDPNU;*/

----------------------------------------------------------------------------------
-- eemaldan unresolve'st  V E D PN ! UNIT !
----------------------------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_entry; --62748,55777

DELETE
FROM run_201909021100_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909021100_optimistic_matching_VEDPNU);

select count(*)
from run_201909021100_unresolved_entry; --37748,31903,24751, 24223
---------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_html; --64207, 61153, 58009, 55704

DELETE
FROM run_201909021100_unresolved_html
WHERE id in (SELECT id_html FROM run_201909021100_optimistic_matching_VEDPNU);

select count(id)
from run_201909021100_unresolved_html;--39208,36694,34398,27138,24833, 24305
----------------------------------------------------------------------------------


----------------------------------------------------------------------------------
-- 3.3 OPTIMISTLIK MATCHIMINE  V E D PN
----------------------------------------------------------------------------------

-- kõik ülejäänud VEDPN-st, mis UNITI tõttu ei matchinud ära
-- 523 ÜLE
select distinct analysis_name_html,
                analysis_name_entry,
                parameter_unit_html,
                parameter_unit_entry,
                reference_values_entry,
                reference_values_html,
                value_html,
                value_entry
from run_201909021100_optimistic_matching_VEDPN
-- MATCHIMATA READ
where id_html in (select id from run_201909021100_unresolved_html)
   or id_entry in (select id from run_201909021100_unresolved_entry);

--id_html= 2367878
--------------------------------------------------------------------------------------
--------------------------------------------!!!!!!!!!!!!!!!!!!!!!!-------------------------------------

----------------------------------------------------------------------------------
-- 4 OPTIMISTLIK MATCHIMINE  V E  D  PN
----------------------------------------------------------------------------------
select *
from run_201909021100_optimistic_matching_VEDPN
-- MATCHIMATA READ
where id_html in (select id from run_201909021100_unresolved_html)
   or id_entry in (select id from run_201909021100_unresolved_entry);

-- id, kus ühele HTML vastab mitu ENTRY rida
-- 380 distinc html id-l vastab mitu entry rida, 518, 124
select distinct id_html, id_entry
from (
         SELECT distinct
                id_html,
                id_entry,
                ROW_NUMBER() OVER (PARTITION BY id_html ORDER BY id_html) as row_number
         FROM run_201909021100_optimistic_matching_VEDPN
         --order by id_html, id_entry
     ) as tbl
where row_number > 1;

-- näide 1.2 :üks html, kolm entryt
--1041860,29869
--1041860,29870
--1041860,29871

-- näide 1.1
--üks html, kaks entryt
--2173333,61153
--2173333,61159

--näide 1.3
--2171725,56057
--2171725,56258

---------------------------------------------------------------------------------------
-- NÄIDE 1.1
--üks html, kaks entryt
--2173333,61153
--2173333,61159
---------------------------------------------------------------------------------------
-- HTML
select *
from run_201909021100_analysis_html_loinced_unique
where id = 2173333;

select epi_id, value, parameter_name, parameter_unit, effective_time, analysis_name, reference_values
from run_201909021100_analysis_html_loinced_unique
where epi_id = '37826116' and value = '58.0'
order by value ;


--ENTRY
select *
from run_201909021100_analysis_entry_loinced_unique
where id = 61153 or id = 61159;

select epi_id, value, parameter_name, parameter_unit, effective_time, analysis_name, reference_values
from run_201909021100_analysis_entry_loinced_unique
where epi_id = '37826116' and value = '58.0'
order by value ;



---------------------------------------------------------------------------------------
-- NÄIDE 1.2
-- ÜHELE HTML VASTAB 3 ENTRYT
-- LIPS
---------------------------------------------------------------------------------------
--1041860,29869
--1041860,29870
--1041860,29871

--HTML 1
select *
from run_201909021100_analysis_html_loinced_unique
where id = 1041860;

select  epi_id, value, parameter_name, parameter_name_raw, parameter_unit, effective_time, analysis_name, reference_values
from run_201909021100_analysis_html_loinced_unique
where epi_id = '19289924' and value = '-0.4';

--ENTRY 3
select *
from run_201909021100_analysis_entry_loinced_unique
where id = 29869 or id = 29870 or id = 29871;

select epi_id, value, parameter_name, parameter_name_raw,  parameter_unit, effective_time, analysis_name, reference_values
from run_201909021100_analysis_entry_loinced_unique
where epi_id = '19289924' and value = '-0.4';

--------------------------------------------------------------------------------------
-- NÄIDE 1.2
-- ÜHELE HTML VASTAB 2 ENTRYT
---------------------------------------------------------------------------------------
--2171725,56057
--2171725,56258

--HTML 1
select *
from run_201909021100_analysis_html_loinced_unique
where id = 2171725;

select  id, epi_id, value, parameter_name, parameter_unit, effective_time, analysis_name, reference_values
from run_201909021100_analysis_html_loinced_unique
where epi_id = '37826116' and value = '7.447' and id = 2171725;

--ENTRY 2
select *
from run_201909021100_analysis_entry_loinced_unique
where id = 56057 or id = 56258;

select id, epi_id, value, parameter_name, parameter_unit, effective_time, analysis_name, reference_values
from run_201909021100_analysis_entry_loinced_unique
where epi_id = '37826116' and value = '7.447' and (id = 56057 or id = 56258);




--------------------------------------------------------------------------------------
-- id, kus ühele ENTRY-le vastab mitu HTML rida
-- 378 distinc html id-l vastab mitu entry rida, nüüd 2487, 512, 392!!!!
select id_entry, id_html, row_number
from (
         SELECT id_html,
                id_entry,
                ROW_NUMBER() OVER (PARTITION BY id_entry ORDER BY id_entry) as row_number
         FROM (select distinct id_html, id_entry from run_201909021100_optimistic_matching_VEDPN) as tbl
         --where id_html = 4149308
         order by id_entry, id_html
     ) as tbl
where row_number > 1;

---------------------------------------------------------------------------------------
-- NÄIDE 1 KAHVEL
---------------------------------------------------------------------------------------
--id järgi kahvel
select *
from run_201909021100_analysis_entry_loinced_unique
where id = 6807;
select *
from run_201909021100_analysis_html_loinced_unique
where id = 4149271;

-- info järgi kahvel
-- KOMMENTAAR: HMTL tabelis ongi nagu ühte paneeli topelt
-- 1 rida
select *
from run_201909021100_analysis_entry_loinced_unique
where epi_id = '33055532'
  and parameter_name_raw = 'P-ALAT    alaniini aminotransferaas'
  and value_raw = '22';
-- 2 rida
select *
from run_201909021100_analysis_html_cleaned_unique
where epi_id = '33055532'
  and parameter_name_raw = 'P-ALAT alaniini aminotransferaas'
  and value_raw = '22';

select * from original.analysis where epi_id = '33055532';

---------------------------------------------------------------------------------------
-- NÄIDE 2 KAHVEL
---------------------------------------------------------------------------------------
--id järgi kahvel
select *
from run_201909021100_analysis_entry_loinced_unique
where id = 10984;
select *
from run_201909021100_analysis_html_loinced_unique
where id = 3935022;

-- info järgi kahvel
-- KOMMENTAAR: HMTL tabelis ongi nagu ühte paneeli topelt, KAS DUPLICATE DETECTIONIGA PEAKSIN EEMALDAMA? Hetkel võtab arvesse paneeli id ja selle järgi loeb erinevateks
-- 1 rida
select *
from run_201909021100_analysis_entry_loinced_unique
where epi_id = '24333195'
  and parameter_name = 'Hemoglobiin'
  and value_raw = '105';
-- 2 rida
select *
from run_201909021100_analysis_html_cleaned_unique
where epi_id = '24333195'
  and parameter_name = 'Hemoglobiin'
  and value_raw = '105';

select * from original.analysis where epi_id = '24333195';

---------------------------------------------------------------------------------------
-- NÄIDE 3 KAHVEL
---------------------------------------------------------------------------------------
--id järgi kahvel
select *
from run_201909021100_analysis_entry_loinced_unique
where id = 60329;
select *
from run_201909021100_analysis_html_loinced_unique
where id = 3084072;

-- info järgi kahvel
-- KOMMENTAAR: HMTL tabelis on ühel OLEMAS reference value ja teisel PUUDU
-- 1 rida entrys
select *
from run_201909021100_analysis_entry_loinced_unique
where epi_id = '52408383'
  and parameter_name = 'pO2'
  and value= '89.3'
  --and reference_values is NULL;
-- 2 rida
select *
from run_201909021100_analysis_html_cleaned_unique
where epi_id = '52408383'
  and parameter_name = 'pO2'
  and value = '89.3'
  --and reference_values is NULL;;

select * from original.analysis where epi_id = '24333195';


--------------------------------------------------------------------------------------
--5. LIPSU TUVASTAMINE
---------------------------------------------------------------------------------------
-- html|entry
--  a -- b
--    \/
--    /\
--  c -- d

create table xxx_html as
    select  epi_id, value, parameter_name, parameter_name_raw, parameter_unit, effective_time, analysis_name, reference_values
    from run_201909021100_analysis_html_loinced_unique
    where epi_id = '19289924' and value = '-0.4';

create table xxx_entry as
    select epi_id, value, parameter_name, parameter_name_raw,  parameter_unit, effective_time, analysis_name, reference_values
    from run_201909021100_analysis_entry_loinced_unique
    where epi_id = '19289924' and value = '-0.4';

create table xxx_vepn as
    select * From run_201909021100_optimistic_matching_VEDPN
     where epi_id_html = '19289924' and value_html = '-0.4';

drop table if exists run_201909021100_lipsud;
create table run_201909021100_lipsud as
with html_set as (
    select array_agg(distinct id_html) as id_html,
           epi_id_html,
           value_html,
           parameter_name_html,
           --parameter_unit_html,
           effective_time_html,
           count(*) as count_html
    from run_201909021100_optimistic_matching_VEDPN
    group by epi_id_html, value_html, parameter_name_html, effective_time_html
),
     entry_set as (
         select array_agg(distinct id_entry) as id_entry,
                epi_id_entry,
                value_entry,
                parameter_name_entry,
                --parameter_unit_entry,
                effective_time_entry,
                count(*) as count_entry
         from run_201909021100_optimistic_matching_VEDPN -- VEDPN
         group by epi_id_entry, value_entry, parameter_name_entry, effective_time_entry
)
select * from entry_set as e, html_set as h
where e.count_entry = h.count_html
    and e.count_entry > 1  -- ei taha üks ühele seoseid
    and parameter_name_entry = parameter_name_html
    and value_entry = value_html
    and effective_time_entry = effective_time_html
    and epi_id_entry = epi_id_html;

select epi_id_html, value_html, parameter_name_html, parameter_unit_html, effective_time_html, analysis_name_html,
       epi_id_entry, value_entry, parameter_name_entry, parameter_unit_entry, effective_time_entry
 from run_201909021100_optimistic_matching_VEDPN
where parameter_name_raw_html != parameter_name_html;

select * from run_201909021100_analysis_html_loinced where epi_id ='25060468' and value = '7.9' and parameter_name = 'B-Gluc'
select * from run_201909021100_analysis_entry_loinced where epi_id ='25060468' and value = '7.9' and parameter_name = 'B-Gluc'


select * from run_201909021100_analysis_html_loinced_unique
where epi_id = '19289924' and value = '101.3' and parameter_unit = 'mmHg';

select *from run_201909021100_analysis_entry_loinced_unique
where epi_id = '19289924' and value = '101.3'  and parameter_unit = 'mmHg';



----------------------------------------------------------------------------------
-- 6. UNRESOLVE VÄHENDAMINE,
-- eemaldame VEDPN lipsud
----------------------------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_entry; --55777

DELETE
FROM run_201909021100_unresolved_entry
WHERE id in (SELECT unnest(id_entry) FROM run_201909021100_lipsud);

select count(*)
from run_201909021100_unresolved_entry;
--55714, 24223
---------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_html; --55704,24305

DELETE
FROM run_201909021100_unresolved_html
WHERE id in (SELECT unnest(id_html) FROM run_201909021100_lipsud);

select count(*)
from run_201909021100_unresolved_html; --24305


----------------------------------------------------------------------------------
-- 7. E P D matchimine, kui value_html = null ja value_entry = null
----------------------------------------------------------------------------------
--844, 1694
drop table if exists run_201909021100_matching_EPD;
create table run_201909021100_matching_EPD as
with epd_matched as (
    select html.id                          as id_html,
       entry.id                         as id_entry,
       html.row_nr                      as row_nr_html,
       html.epi_id                      as epi_id_html,
       html.epi_type                    as epi_type_html,
       html.parse_type                  as parse_type_html,
       html.panel_id                    as panel_id_html,
       html.analysis_name_raw           as analysis_name_raw_html,
       html.parameter_name_raw          as parameter_name_raw_html,
       html.parameter_unit_raw          as parameter_unit_raw_html,
       html.reference_values_raw        as reference_values_raw_html,
       html.value_raw                   as value_raw_html,
       html.analysis_substrate_raw      as analysis_substrate_raw_html,
       html.analysis_substrate          as analysis_substrate_html,
       html.entry_match_id              as entry_match_id_html,
       html.entry_html_match_desc       as entry_html_match_desc_html,
       html.effective_time_raw          as effective_time_raw_html,
       html.cleaned_value               as cleaned_value_html,
       html.analysis_name               as analysis_name_html,
       html.parameter_name              as parameter_name_html,
       html.effective_time              as effective_time_html,
       html.value                       as value_html,
       html.parameter_unit_from_suffix  as parameter_unit_from_suffix_html,
       html.suffix                      as suffix_html,
       html.value_type                  as value_type_html,
       html.reference_values            as reference_values_html,
       html.parameter_unit              as parameter_unit_html,
       html.loinc_unit                  as loinc_unit_html,
       html.elabor_t_lyhend             as elabor_t_lyhend_html,
       html.loinc_code                  as loinc_code_html,
       html.source                      as source_html,
       entry.original_analysis_entry_id,
       entry.epi_id                     as epi_id_entry,
       entry.epi_type                   as epi_type_entry,
       entry.analysis_id                as analysis_id_entry,
       entry.code_system                as code_system_entry,
       entry.code_system_name           as code_system_name_entry,
       entry.analysis_code_raw          as analysis_code_raw_entry,
       entry.analysis_name_raw          as analysis_name_raw_entry,
       entry.parameter_code_raw         as parameter_code_raw_entry,
       entry.parameter_name_raw         as parameter_name_raw_entry,
       entry.parameter_unit_raw         as parameter_unit_raw_entry,
       entry.reference_values_raw       as reference_values_raw_entry,
       entry.effective_time_raw         as effective_time_raw_entry,
       entry.value_raw                  as value_raw_entry,
       entry.cleaned_value              as cleaned_value_entry,
       entry.analysis_name              as analysis_name_entry,
       entry.parameter_name             as parameter_name_entry,
       entry.effective_time             as effective_time_entry,
       entry.value                      as value_entry,
       entry.parameter_unit_from_suffix as parameter_unit_from_suffix_entry,
       entry.suffix                     as suffix_entry,
       entry.value_type                 as value_type_entry,
       entry.reference_values           as reference_values_entry,
       entry.parameter_unit             as parameter_unit_entry,
       entry.loinc_unit                 as loinc_unit_entry,
       entry.elabor_t_lyhend            as elabor_t_lyhend_entry,
       entry.loinc_code                 as loinc_code_entry,
       entry.source                     as source_entry
    from run_201909021100_analysis_html_loinced_unique as html
             join
         run_201909021100_analysis_entry_loinced_unique as entry
         on html.epi_id = entry.epi_id and
            html.parameter_name = entry.parameter_name and
            date(html.effective_time) = date(entry.effective_time)
    --unresolved rows
    where (html.id in (select * from run_201909021100_unresolved_html) or
           entry.id in (select * from run_201909021100_unresolved_entry))
)
select * from epd_matched
where value_html is null and value_entry is null;
--206 rida, kus E P D lähevad kokku ja väärtused on lihtsalt mõelmas tabelis NULLid

--206
select count(*) from run_201909021100_matching_EPD;

-----------------------------------------------------------------------------------------------------------
--EPD KAHVEL
------------------------------------------------------------------------------------------------------------
-- näide
-- id_html, id_entry,
-- 4060022, 9939,
-- 4059982, 9939,

select epi_id, parameter_name, value, parameter_unit, effective_time, analysis_name, reference_values
from run_201909021100_analysis_html_loinced_unique
where id = 4059943 or id = 4059982;

select epi_id, parameter_name, value,  parameter_unit, effective_time,analysis_name, reference_values
from run_201909021100_analysis_entry_loinced_unique
where  id = 9939;

-- ENTRY ID, mis tekitavad kahvelid
with epd_entry_kahvel as (
    select id_entry
    from run_201909021100_matching_EPD
    group by id_entry
    having count(*) >= 2),
     -- HMTL ID, mis tekitavad kahvleid
     epd_html_kahvel as (
         select id_html
         from run_201909021100_matching_EPD
         group by id_html
         having count(*) >= 2)
-- üks ühele matchitud (EI SISALDA KAHVLEID)
/*select id_entry, id_html
from run_201909021100_matching_EPD
where id_entry not in (select id_entry from epd_entry_kahvel)
  and id_html not in (select id_html from epd_html_kahvel);*/
--lisame EPD järgi matchitud lõplikku tabelisse
insert into run_201909021100_matched
select *, 'epi_id, parameter_name, effective_time, value_html = NULL, value_entry = NULL' as match_descrition
from run_201909021100_matching_EPD
where id_entry not in (select id_entry from epd_entry_kahvel)
  and id_html not in (select id_html from epd_html_kahvel);

--need on kahvlid
select * from run_201909021100_unresolved_html
where id = 4059943 or id = 4059982;
select * from run_201909021100_unresolved_entry
where id = 9939;

---------------------------------------------------------------------------------------------
--eemaldame  EPD unresolvdest
----------------------------------------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_entry; --24223

--tegelt where in teeb veidi üleliigset tööd
DELETE
FROM run_201909021100_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909021100_matched);

select count(*)
from run_201909021100_unresolved_entry;-- 24033
---------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_html; --24305

DELETE
FROM run_201909021100_unresolved_html
WHERE id in (SELECT id_html FROM run_201909021100_matched);

select count(*)
from run_201909021100_unresolved_html; --24115

--resolvitud 3999417 rida
select count(*) from run_201909021100_analysis_html_loinced_unique
where id not in (select id from run_201909021100_unresolved_html);

--need peaks endiselt olema kahvlid
select * from run_201909021100_unresolved_html
where id = 4059943 or id = 4059982;
select * from run_201909021100_unresolved_entry
where id = 9939;

------------------------------------------------------------------------------------
--8. matchimine V E P (kuupäeva viga)
------------------------------------------------------------------------------------
drop table if exists run_201909021100_matching_vep;
create table run_201909021100_matching_vep as
with vep_matched as (
    select html.id                          as id_html,
       entry.id                         as id_entry,
       html.row_nr                      as row_nr_html,
       html.epi_id                      as epi_id_html,
       html.epi_type                    as epi_type_html,
       html.parse_type                  as parse_type_html,
       html.panel_id                    as panel_id_html,
       html.analysis_name_raw           as analysis_name_raw_html,
       html.parameter_name_raw          as parameter_name_raw_html,
       html.parameter_unit_raw          as parameter_unit_raw_html,
       html.reference_values_raw        as reference_values_raw_html,
       html.value_raw                   as value_raw_html,
       html.analysis_substrate_raw      as analysis_substrate_raw_html,
       html.analysis_substrate          as analysis_substrate_html,
       html.entry_match_id              as entry_match_id_html,
       html.entry_html_match_desc       as entry_html_match_desc_html,
       html.effective_time_raw          as effective_time_raw_html,
       html.cleaned_value               as cleaned_value_html,
       html.analysis_name               as analysis_name_html,
       html.parameter_name              as parameter_name_html,
       html.effective_time              as effective_time_html,
       html.value                       as value_html,
       html.parameter_unit_from_suffix  as parameter_unit_from_suffix_html,
       html.suffix                      as suffix_html,
       html.value_type                  as value_type_html,
       html.reference_values            as reference_values_html,
       html.parameter_unit              as parameter_unit_html,
       html.loinc_unit                  as loinc_unit_html,
       html.elabor_t_lyhend             as elabor_t_lyhend_html,
       html.loinc_code                  as loinc_code_html,
       html.source                      as source_html,
       entry.original_analysis_entry_id,
       entry.epi_id                     as epi_id_entry,
       entry.epi_type                   as epi_type_entry,
       entry.analysis_id                as analysis_id_entry,
       entry.code_system                as code_system_entry,
       entry.code_system_name           as code_system_name_entry,
       entry.analysis_code_raw          as analysis_code_raw_entry,
       entry.analysis_name_raw          as analysis_name_raw_entry,
       entry.parameter_code_raw         as parameter_code_raw_entry,
       entry.parameter_name_raw         as parameter_name_raw_entry,
       entry.parameter_unit_raw         as parameter_unit_raw_entry,
       entry.reference_values_raw       as reference_values_raw_entry,
       entry.effective_time_raw         as effective_time_raw_entry,
       entry.value_raw                  as value_raw_entry,
       entry.cleaned_value              as cleaned_value_entry,
       entry.analysis_name              as analysis_name_entry,
       entry.parameter_name             as parameter_name_entry,
       entry.effective_time             as effective_time_entry,
       entry.value                      as value_entry,
       entry.parameter_unit_from_suffix as parameter_unit_from_suffix_entry,
       entry.suffix                     as suffix_entry,
       entry.value_type                 as value_type_entry,
       entry.reference_values           as reference_values_entry,
       entry.parameter_unit             as parameter_unit_entry,
       entry.loinc_unit                 as loinc_unit_entry,
       entry.elabor_t_lyhend            as elabor_t_lyhend_entry,
       entry.loinc_code                 as loinc_code_entry,
       entry.source                     as source_entry
    from run_201909021100_analysis_html_loinced_unique as html
             join
         run_201909021100_analysis_entry_loinced_unique as entry
         on html.epi_id = entry.epi_id and
            html.parameter_name = entry.parameter_name and
            html.value = entry.value
         --unresolved rows
    where (html.id in (select * from run_201909021100_unresolved_html) or
           entry.id in (select * from run_201909021100_unresolved_entry))
)
select *
from vep_matched
where effective_time_html is null
   or effective_time_entry is null;

--45
select count(*) from run_201909021100_matching_vep;

select id_html, id_entry, effective_time_html, effective_time_entry from run_201909021100_matching_vep
order by id_html, id_entry;



-----------------------------------------------------------------------------------------------------------
-- V E P KAHVEL (kuupäeva viga)
--PROBLEEM:
-- ühele HTML mitu entryt:
------------------------------------------------------------------------------------------------------------
--EPI_ID; id_html, id_entry, PN, VALUE, EFFECTIVE_TIME, ref value, PU
--50237887, 2962412,66119,Kaltsium arteriaalses veres,1.13,2015-11-19 00:00:00.000000,,"Kaltsium (ioniseeritud, pH 7,4) arteriaalses veres",,mmol/L
-- kõik sam, 2962412,66127

-- näide: html = 2962412, entry = 66119, 66127
select *
from run_201909021100_matching_vep
where id_html = 2962412;

-- ENTRY ID, mis tekitavad kahvelid
with vep_entry_kahvel as (
    select id_entry
    from run_201909021100_matching_vep
    group by id_entry
    having count(*) >= 2),
     -- HMTL ID, mis tekitavad kahvleid
     vep_html_kahvel as (
         select id_html
         from run_201909021100_matching_vep
         group by id_html
         having count(*) >= 2)
-- üks ühele matchitud (EI SISALDA KAHVLEID)
--lisame EPD järgi matchitud lõplikku tabelisse
insert into run_201909021100_matched
select *, 'epi_id, parameter_name, value, effective_time_html = NULL or effective_time_entry = NULL' as match_descrition
from run_201909021100_matching_vep
where id_entry not in (select id_entry from vep_entry_kahvel)
  and id_html not in (select id_html from vep_html_kahvel);

--need on kahvlid
select * from run_201909021100_unresolved_html
where id = 2962412;
select * from run_201909021100_unresolved_entry
where id = 66119 or id = 66127;




---------------------------------------------------------------------------------------------
--eemaldame  EPD unresolvdest
----------------------------------------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_entry; --24033

--tegelt where in teeb veidi üleliigset tööd
DELETE
FROM run_201909021100_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909021100_matched);

select count(*)
from run_201909021100_unresolved_entry;-- 23990
---------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_html; --24115

DELETE
FROM run_201909021100_unresolved_html
WHERE id in (SELECT id_html FROM run_201909021100_matched);

select count(*)
from run_201909021100_unresolved_html; --24072






------------------------------------------------------------------------------------
--9. matchimine V E D (parameter_name viga), kus pn_entry = NULL or pn_html = NULL
------------------------------------------------------------------------------------
-- peamiselt juhud, kus HTMLs on PN sattunud AN alla

drop table if exists run_201909021100_matching_VED;
create table run_201909021100_matching_VED as
with ved_matched as (
    select html.id                          as id_html,
       entry.id                         as id_entry,
       html.row_nr                      as row_nr_html,
       html.epi_id                      as epi_id_html,
       html.epi_type                    as epi_type_html,
       html.parse_type                  as parse_type_html,
       html.panel_id                    as panel_id_html,
       html.analysis_name_raw           as analysis_name_raw_html,
       html.parameter_name_raw          as parameter_name_raw_html,
       html.parameter_unit_raw          as parameter_unit_raw_html,
       html.reference_values_raw        as reference_values_raw_html,
       html.value_raw                   as value_raw_html,
       html.analysis_substrate_raw      as analysis_substrate_raw_html,
       html.analysis_substrate          as analysis_substrate_html,
       html.entry_match_id              as entry_match_id_html,
       html.entry_html_match_desc       as entry_html_match_desc_html,
       html.effective_time_raw          as effective_time_raw_html,
       html.cleaned_value               as cleaned_value_html,
       html.analysis_name               as analysis_name_html,
       html.parameter_name              as parameter_name_html,
       html.effective_time              as effective_time_html,
       html.value                       as value_html,
       html.parameter_unit_from_suffix  as parameter_unit_from_suffix_html,
       html.suffix                      as suffix_html,
       html.value_type                  as value_type_html,
       html.reference_values            as reference_values_html,
       html.parameter_unit              as parameter_unit_html,
       html.loinc_unit                  as loinc_unit_html,
       html.elabor_t_lyhend             as elabor_t_lyhend_html,
       html.loinc_code                  as loinc_code_html,
       html.source                      as source_html,
       entry.original_analysis_entry_id,
       entry.epi_id                     as epi_id_entry,
       entry.epi_type                   as epi_type_entry,
       entry.analysis_id                as analysis_id_entry,
       entry.code_system                as code_system_entry,
       entry.code_system_name           as code_system_name_entry,
       entry.analysis_code_raw          as analysis_code_raw_entry,
       entry.analysis_name_raw          as analysis_name_raw_entry,
       entry.parameter_code_raw         as parameter_code_raw_entry,
       entry.parameter_name_raw         as parameter_name_raw_entry,
       entry.parameter_unit_raw         as parameter_unit_raw_entry,
       entry.reference_values_raw       as reference_values_raw_entry,
       entry.effective_time_raw         as effective_time_raw_entry,
       entry.value_raw                  as value_raw_entry,
       entry.cleaned_value              as cleaned_value_entry,
       entry.analysis_name              as analysis_name_entry,
       entry.parameter_name             as parameter_name_entry,
       entry.effective_time             as effective_time_entry,
       entry.value                      as value_entry,
       entry.parameter_unit_from_suffix as parameter_unit_from_suffix_entry,
       entry.suffix                     as suffix_entry,
       entry.value_type                 as value_type_entry,
       entry.reference_values           as reference_values_entry,
       entry.parameter_unit             as parameter_unit_entry,
       entry.loinc_unit                 as loinc_unit_entry,
       entry.elabor_t_lyhend            as elabor_t_lyhend_entry,
       entry.loinc_code                 as loinc_code_entry,
       entry.source                     as source_entry
    from run_201909021100_analysis_html_loinced_unique as html
             join
         run_201909021100_analysis_entry_loinced_unique as entry
         on html.epi_id = entry.epi_id and
            date(html.effective_time) = date(entry.effective_time) and
            html.value = entry.value
         --unresolved rows
    where (html.id in (select * from run_201909021100_unresolved_html) or
           entry.id in (select * from run_201909021100_unresolved_entry))
)
select *
from ved_matched
where parameter_name_entry is null
   or parameter_name_html is null;

--137
select count(*) from run_201909021100_matching_ved;

--näide kus VED matchitud problemaatiliselt
select epi_id_html, parse_type_html, id_html, id_entry, parameter_name_html, parameter_name_entry, analysis_name_html, analysis_name_entry,
       value_html, value_entry,
       parameter_unit_html, parameter_unit_entry, reference_values_html, reference_values_entry,
       effective_time_html, effective_time_entry
from run_201909111308_matching_ved
where reference_values_entry != reference_values_html;

-----------------------------------------------------------------------------------------------------------
-- V E P KAHVEL (kuupäeva viga)
--PROBLEEM:
-- ühele HTML mitu entryt:
------------------------------------------------------------------------------------------------------------
-- probleemsed:  üks id_entry = 8756, kaks id_html = 4910093, 4910073
-- parse_type = 2.2 on halvasti parsitud

-- näide
select epi_id_html, id_html, id_entry, parameter_name_html, parameter_name_entry, analysis_name_html, analysis_name_entry,
       value_html, value_entry,
       parameter_unit_html, parameter_unit_entry, reference_values_html, reference_values_entry,
       effective_time_html, effective_time_entry
from run_201909021100_matching_VED;
--where id_html =4833211;

select * from runfull201903041255_analysis_html where epi_id = '30123933';

select epi_id_html, id_html, id_entry, parameter_name_html, parameter_name_entry, analysis_name_html, analysis_name_entry,
       value_html, value_entry,
       parameter_unit_html, parameter_unit_entry, reference_values_html, reference_values_entry,
       effective_time_html, effective_time_entry from run_201909021100_matching_VED
where id_entry = 8756;

-- MITTE kahvlite lisamine:
-- ENTRY ID, mis tekitavad kahvelid
with ved_entry_kahvel as (
    select id_entry
    from run_201909021100_matching_ved
    group by id_entry
    having count(*) >= 2),
     -- HMTL ID, mis tekitavad kahvleid
     ved_html_kahvel as (
         select id_html
         from run_201909021100_matching_ved
         group by id_html
         having count(*) >= 2)
-- üks ühele matchitud (EI SISALDA KAHVLEID)
--lisame VED järgi matchitud lõplikku tabelisse
insert into run_201909021100_matched
select *, 'epi_id, effective_time, value, parameter_name_entry = NULL or parameter_name_html = NULL' as match_description
from run_201909021100_matching_ved
where id_entry not in (select id_entry from ved_entry_kahvel)
  and id_html not in (select id_html from ved_html_kahvel);
--117

select count(*) from run_201909021100_matched;--4000355

--need on kahvlid
select * from run_201909021100_unresolved_html
where  id = 4910093 or id = 4910073;

select * from run_201909021100_unresolved_entry
where id = 8756;




---------------------------------------------------------------------------------------------
--eemaldame VED unresolvdest
----------------------------------------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_entry; --23990

--tegelt where in teeb veidi üleliigset tööd
DELETE
FROM run_201909021100_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909021100_matched);

select count(*)
from run_201909021100_unresolved_entry;-- 23873
---------------------------------------------------------------
select count(*)
from run_201909021100_unresolved_html; --24072

DELETE
FROM run_201909021100_unresolved_html
WHERE id in (SELECT id_html FROM run_201909021100_matched);

select count(*)
from run_201909021100_unresolved_html; --23955

-- need kahvlid peaksid ENDISELT OLEMA unresolvedes alles
select * from run_201909021100_unresolved_html
where  id = 4910093 or id = 4910073;
select * from run_201909021100_unresolved_entry
where id = 8756;

------------------------------------------------------------------------------------------------------
-- 10. mis sinna unresolvde jäänud?
------------------------------------------------------------------------------------------------------
--vaatame nt epikriisi 9888915
--tulemus :
-- Kliiniline vere ja glükohemoglobiini analüüs paistab olevat HTML's topelt ehk 56 rida, samas kui entrys on 28
-- ja epicirsis viewris on 28
-- HTML tekitab duplikaatse rea, kus value = NULL, effective_time = NULL

select * from run_201909021100_analysis_html_loinced_unique
where id in (select id from run_201909021100_unresolved_html) and
      epi_id = '9888915';

select * from run_201909021100_analysis_entry_loinced_unique
where id in (select id from run_201909021100_unresolved_entry) and
      epi_id = '9888915';

-- epicisis vieweri järgi tundub, et tegelikkuses peaks olema 70 rida ehk HTML parsimises miskit valesti?
select * from run_201909021100_analysis_entry_loinced_unique where epi_id = '9888915'; --70 rida entrys
select * from run_201909021100_analysis_html_loinced_unique where epi_id = '9888915'; -- 128 rida entrys

-- Kliiniline vere ja glükohemoglobiini analüüs paistab olevat HTML's topelt ehk 56 rida, samas kui entrys on 28
-- ja epicirsis viewris on 28
-- HTML tekitab duplikaatse rea, kus value = NULL, effective_time = NULL
select * from run_201909021100_analysis_html_loinced_unique
where epi_id = '9888915' and analysis_name_raw = 'Kliiniline vere ja glükohemoglobiini analüüs';

select * from run_201909021100_analysis_entry_loinced_unique
where epi_id = '9888915' and analysis_name_raw = 'Kliiniline vere ja glükohemoglobiini analüüs';


------------------------------------------------------------------------------------------------------------
--vaatame entry_id = 16372  , epi_id = 8716467
-- ENTRY millegipärast täiesti vale PN?????

-- tulemus:
-- PN erinevad ENTRY  : parameter_name_raw = B-CBC-5DIFF-NRBC-RET
--             -- HTML : parameter_name_raw = P-LCR

--91 rida ENTRY
select * from run_201909021100_analysis_entry_loinced_unique
where epi_id = '8716467';

-- 94 rida HTML
select * from run_201909021100_analysis_html_loinced_unique
where epi_id = '8716467';

--unresolved on entry id = 16372
select epi_id, id, analysis_name_raw, parameter_name_raw, parameter_unit_raw, value_raw, effective_time_raw
from run_201909021100_analysis_entry_loinced_unique where id = 16372;

-- vaatame, mis html-s sellele vastab
select  epi_id, parse_type, id, analysis_name_raw, parameter_name_raw, parameter_unit_raw, value_raw, effective_time_raw
from run_201909021100_analysis_html_loinced_unique
where epi_id = '8716467'
    and value_raw = '17,6'
    and analysis_name_raw = 'Hemogramm viieosalise leukogrammiga, normoblastidega ja retikulotsüütidega';

select * from run_201909021100_analysis_html_loinced_unique
where epi_id = '8716467'
    and parameter_name_raw = 'P-LCR';

select * from run_201909021100_analysis_entry_loinced_unique
where epi_id = '8716467' and value_raw = '17,6';

-- Entrys valed parameter_name_raw, tähistatud analysis name lühendiga!!
select parameter_name_raw from run_201909021100_analysis_entry_loinced_unique
where epi_id = '8716467';

-- PN erinevad ENTRY  : parameter_name_raw = B-CBC-5DIFF-NRBC-RET
            -- HTML : parameter_name_raw = P-LCR



----------------------------------------------------------------------------------------------------------------
-- näide epi_id =  8244450, html_id = 3466249
--tulemus: html loen ka tühjad value_raw väljad sisse, entrys need välja jäetud

select * from run_201909021100_unresolved_html;

--entrys selle epiga 99 rida, HTML's 565 rida
select * from run_201909021100_analysis_html_loinced_unique
where epi_id = '8244450';
select * from run_201909021100_analysis_entry_loinced_unique
where epi_id = '8244450';

--entrys Biokeemia analüüse 14
--htmls  Biokeemia analüüse 102
--vieweris Biokeemia analüüse 18 * 6 tabel umbes = 108
select * from run_201909021100_analysis_html_loinced_unique
where epi_id = '8244450' and analysis_name = 'Biokeemia analüüs';
select * from run_201909021100_analysis_entry_loinced_unique
where epi_id = '8244450' and analysis_name = 'Biokeemia analüüs';

select * from run_201909021100_analysis_entry_loinced_unique
where epi_id = '8244450' and analysis_name_raw = 'Biokeemia analüüs';






----------------------------------------------------------------------------------------------------------------------
-- kas unresolvedes on selliseid, mis peaks olema lahendatavad?
-- et entrys kui ka htmls on selle epi_id'ga sama palju rida

-- need on IDEAALSED MATCHID RIDADEGA epikriisid
create table xxx_perfect_match_rows as
with html_epi_count as
         (select epi_id as html_epi_id, count(*) as html_count
          from run_201909021100_analysis_html_loinced_unique
          group by epi_id),
     entry_epi_count as
         (select epi_id as entry_epi_id, count(*) as entry_count
          from work.run_201909021100_analysis_entry_loinced_unique
          group by epi_id)
select *
from html_epi_count --select html_epi_id, entry_epi_id, html_count, entry_count
         inner join entry_epi_count
                    on html_epi_id = entry_epi_id
where entry_count = html_count;

-- vaatame kas mõni ideaalne epikriis on unresolved
select * from run_201909021100_analysis_html_loinced_unique
where epi_id in (select html_epi_id from xxx_perfect_match_rows) --epikriis peaks olema perfektne
    and id in (select id from run_201909021100_unresolved_html); -- millegipärast siiski unresolved
--lausa 3385 sellist juhtu
-- selline mulje, et olen kuskil unustanud matched tabelisse asju lisada

--nt epi_id = 834428
--ükski pole matchitud
select * from run_201909021100_matched where epi_id_html = '834428';

select analysis_name, parameter_name, parameter_unit, value, effective_time, reference_values
    from run_201909021100_analysis_entry_loinced_unique where epi_id = '834428'; --31 rida
select  analysis_name, parameter_name, parameter_unit, value, effective_time, reference_values
    from run_201909021100_analysis_entry_loinced_unique where epi_id = '834428'; --31 rida




























------------------------------------------------------------------------------------
-- V D E P R
------------------------------------------------------------------------------------
select parameter_name_html, parameter_name_entry, parameter_unit_html, parameter_unit_entry, effective_time_html, effective_time_entry, value_html, value_entry, analysis_name_html, analysis_name_entry, reference_values_html
from run_201909021100_optimistic_matching_VEDPN
where reference_values_html = reference_values_entry;

------------------------------------------------------------------------------------
-- V D E P A
------------------------------------------------------------------------------------







----------------------------------------------------------------------------------
-- 4. UNRESOLVE VÄHENDAMINE ???
-- eemaldame unresolvedes resolved_read = optimistic matching V D E P U??
----------------------------------------------------------------------------------
select count(*)
from run_201908261300_unresolved_entry; --62748

DELETE
FROM run_201908261300_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201908261300_optimistic_matching_VEDPU);

select count(*)
from run_201908261300_unresolved_entry;
--59902
---------------------------------------------------------------
select count(*)
from run_201908261300_unresolved_html; --64207

DELETE
FROM run_201908261300_unresolved_html
WHERE id in (SELECT id_html FROM run_201908261300_optimistic_matching_VEDPU);

select count(id)
from run_201908261300_unresolved_html;
--61362

----------------------------------------------------------------
-- KUUPÄEVA ERALDAMINE
select date(CAST('2010-10-29 07:00:00.000000' AS timestamp));



/*
select * from
    (select * from run_201908261300_analysis_entry_loinced as entry
        where epi_id in (select epi_id from run_201908261300_unresolved_entry)
        limit 100) as entry,
    (select * from run_201908261300_analysis_html_loinced
        where epi_id in (select epi_id from run_201908261300_unresolved_html)
        limit 100) as html;
*/

-- ideaalis võiks töötada
/*(select * from run_201908261300_analysis_entry_loinced as entry
    where epi_id in (select epi_id from run_201908261300_unresolved_entry))
join
(select * from run_201908261300_analysis_html_loinced
    where epi_id in (select epi_id from run_201908261300_unresolved_html)) as html
on html.effective_time = entry.effective_time;*/

-- kõik unresolvimata read
-- 396 063 kui value ja epi_id
-- 95 837 kui value, epi_id, parameter_name
select entry.*
from run_201908261300_analysis_entry_loinced as entry
         join run_201908261300_analysis_html_loinced as html
              on html.value = entry.value and
                 html.epi_id = entry.epi_id and
                 html.parameter_name = entry.parameter_name
where html.epi_id in (select html.epi_id from run_201908261300_unresolved_html)
  and entry.epi_id in (select entry.epi_id from run_201908261300_unresolved_html);



--create table work.run_201908261300_to_be_matched as
/*    select *,
        CAST(NULL AS INT) as original_analysis_entry_id,
        CAST(NULL AS INT) as analysis_id ,
        NULL as code_system ,
        NULL as code_system_name ,
        NULL as  analysis_code_raw,
        NULL as  parameter_code_raw ,
        --NULL as html_match_id ,
        --NULL as row_count,
        'html' as source
    from work.run_201908261300_analysis_html_loinced
    union all
    select *,
        CAST(NULL AS INT) AS row_nr,
        NULL AS parse_type,
        CAST(NULL AS INT) AS panel_id,
        NULL AS analysis_substrate,
        NULL AS analysis_substrate_raw,
        CAST(NULL AS INT) AS entry_match_id,
        NULL AS entry_html_match_desc,
        'entry' as source from work.run_201908261300_analysis_entry_loinced;*/


--column types
SELECT array_agg(COLUMN_NAME)--, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'run_201908261300_analysis_html_loinced';

--column types
(SELECT (COLUMN_NAME) --, DATA_TYPE
 FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME = 'run_201908261300_analysis_entry_loinced');



-----------------------------------------------------------------------------------
--VANA
/*
--html columns: row_nr, epi_id, epi_type, parse_type, panel_id, analysis_name_raw, parameter_name_raw, parameter_unit_raw,
                -- reference_values_raw, effective_time_raw, value_raw, analysis_substrate_raw, analysis_name, reference_values,
                --effective_time,analysis_substrate, entry_match_id, entry_html_match_desc, id, value_type, parameter_name, value

--entry veerud

       NULL AS value_type


select * from information_schema.columns
                            where table_schema = 'work' and table_name = 'runfull201902260943_analysis_entry';
-- entrys kokku 24 rida, lisan juurde 7 = 31

select (column_name) from information_schema.columns
                            where table_schema = 'work' and table_name = 'runfull201902260943_analysis_html';
-- html kokku 20 rida, lisan juurde 10 = 30

-- html and  entry
select *,
       NULL as original_analysis_entry_id,
       NULL as analysis_id,
       NULL as code_system,
       NULL as code_system_name,
       NULL as analysis_code_raw,
       NULL as parameter_code_raw,
       NULL as html_match_id,
       NULL as row_count,
       NULL as parameter_name,  -- !!!!
       NULL AS value, -- !!!!!
        NULL AS value_type
from work.runfull201902260943_analysis_html;
--row nr int, panel_id int, effecitvve time_(raw )datetime, entry_match_id int, id int
union;
select *,
       NULL AS row_nr,
       NULL AS parse_type,
       NULL AS panel_id,
       NULL AS analysis_substrate,
       NULL AS analysis_substrate_raw,
       NULL AS entry_match_id,
       NULL AS value_type
from work.runfull201902260943_analysis_entry
limit 10
--id int, original_analysis_entry_id int, analysis_id int, effecitve-time timestap,html_matcjid int,row_count int
*/

-- full outer join epi id järgi
select count(*)
from work.runfull201902260943_analysis_html as h
         full outer join
     work.runfull201902260943_analysis_entry as e
     on h.epi_id = e.epi_id;
--428 685 839

---------------------------------------------------------------------------------------------
-- MATCHIMISE TEST ANDMED
---------------------------------------------------------------------------------------------
---HTML
---------------------------------------------------------------------------------------------

--html: work.run_201907231010analysis_html_cleaned, 5000 rida, 44 erinevat epi_id
drop table if exists work.run_201907231010_html;
create table work.run_201907231010_html as
select *, 'html' as source
from work.run_201907241527_analysis_html_loinced;

ALTER TABLE work.run_201907231010_html
    ADD COLUMN original_analysis_entry_id integer,
    ADD COLUMN analysis_id                integer,
    ADD COLUMN code_system                varchar,
    ADD COLUMN code_system_name           varchar,
    ADD COLUMN analysis_code_raw          varchar,
    ADD COLUMN parameter_code_raw         varchar,
    ADD COLUMN html_match_id              integer,
    ADD COLUMN row_count                  integer
;

select *
from work.runfull201903041255_analysis_html
limit 1;

---------------------------------------------------------------------------------------------
-- see osa peaks järgmise html_parsimisel korda saama
-- puhastaud väärtused milelgipärast effective_time_raw all, mitte effective_time!!!
select effective_time, effective_time_raw
from work.run_201907231010_html;
UPDATE work.run_201907231010_html
SET effective_time = effective_time_raw;

ALTER TABLE work.run_201907231010_html
    ALTER COLUMN effective_time_raw TYPE varchar;
---------------------------------------------------------------------------------------------
-- veergude info

select (column_name)
from information_schema.columns
where table_schema = 'work'
  and table_name = 'run_201907231010_html';


--column types
SELECT array_agg(COLUMN_NAME)--,   DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'run_201907231010_html';

---------------------------------------------------------------------------------------------
-- analysis_name
select analysis_name_raw, count(*)
from work.run_201907231010_analysis_html_loinced
group by analysis_name_raw
order by count desc;


---------------------------------------------------------------------------------------------
---ENTRY
---------------------------------------------------------------------------------------------

-- entry:work.run_201907231010_entry, 7098 rida, sama epi_id-ga read
drop table if exists work.run_201907231010_entry;

create table work.run_201907231010_entry as
select *, 'entry' as source
from work.runfull201902260943_analysis_entry
where epi_id in (select epi_id from work.run_201907231519_analysis_html_loinced);


ALTER TABLE work.run_201907231010_entry
    ADD COLUMN row_nr                 integer,
    ADD COLUMN parse_type             varchar,
    ADD COLUMN panel_id               integer,
    ADD COLUMN analysis_substrate     varchar,
    ADD COLUMN analysis_substrate_raw varchar,
    ADD COLUMN entry_match_id         integer,
    ADD COLUMN value_type             varchar,
    ADD COLUMN loinc_unit             varchar,
    ADD COLUMN elabor_t_lyhend        varchar,
    ADD COLUMN loinc_code             varchar
;


select (column_name)
from information_schema.columns
where table_schema = 'work'
  and table_name = 'run_201907231010_entry'

--column types
SELECT array_agg(COLUMN_NAME) --,  DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'run_201907231010_entry';

----------------------------------------------------------------------------------------
--common table
----------------------------------------------------------------------------------------
drop table if exists work.run_201907231010_joined_entry_html;
create table work.run_201907231010_joined_entry_html as
select row_nr,
       epi_id,
       epi_type,
       parse_type,
       panel_id,
       analysis_name_raw,
       parameter_name_raw,
       parameter_unit_raw,
       reference_values_raw,
       effective_time_raw,
       value_raw,
       analysis_substrate_raw,
       analysis_name,
       reference_values,
       effective_time,
       analysis_substrate,
       entry_match_id,
       entry_html_match_desc,
       id,
       value_type,
       parameter_name,
       value,
       parameter_unit,
       loinc_unit,
       elabor_t_lyhend,
       loinc_code,
       source,
       original_analysis_entry_id,
       analysis_id,
       code_system,
       code_system_name,
       analysis_code_raw,
       parameter_code_raw,
       html_match_id,
       row_count
from work.run_201907231010_html
union all
select row_nr,
       epi_id,
       epi_type,
       parse_type,
       panel_id,
       analysis_name_raw,
       parameter_name_raw,
       parameter_unit_raw,
       reference_values_raw,
       effective_time_raw,
       value_raw,
       analysis_substrate_raw,
       analysis_name,
       reference_values,
       effective_time,
       analysis_substrate,
       entry_match_id,
       entry_html_match_desc,
       id,
       value_type,
       parameter_name,
       value,
       parameter_unit,
       loinc_unit,
       elabor_t_lyhend,
       loinc_code,
       source,
       original_analysis_entry_id,
       analysis_id,
       code_system,
       code_system_name,
       analysis_code_raw,
       parameter_code_raw,
       html_match_id,
       row_count
from work.run_201907231010_entry;

-- both tables have columns: ["analysis_name", "value", "parameter_name", "parameter_unit", "reference_values", "effective_time"]

----------------------------------------------------------------------------------------
-- DUPLICATES
----------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------
-- PERFECT DUPLICATES
------------------------------------------------------------------------------------------
SELECT epi_id,
       analysis_name,
       value,
       parameter_name,
       parameter_unit,
       reference_values_raw,
       effective_time,
       array_agg(source),
       count(*)
FROM work.run_201907231010_joined_entry_html
GROUP BY epi_id, analysis_name, value, parameter_name, parameter_unit, reference_values_raw, effective_time
HAVING count(*) = 2;

select *
from work.run_201907231010_joined_entry_html
where epi_id = '14052071'
  and parameter_name = 'RDW-SD'


--543 duplikaati
SELECT epi_id,
       analysis_name,
       value,
       parameter_name,
       parameter_unit,
       reference_values_raw,
       effective_time,
       array_agg(source),
       count(*)
FROM work.run_201907231010_joined_entry_html
GROUP BY epi_id, analysis_name, value, parameter_name, parameter_unit, reference_values_raw, effective_time
HAVING count(*) > 1;

-- duplikaatsed read
SELECT id, count(*)
FROM work.run_201907231010_joined_entry_html
GROUP BY id
HAVING count(*) > 1;

--10579 distinct rida
select distinct on ( epi_id, analysis_name, value, parameter_name, parameter_unit, reference_values_raw, effective_time) *
from work.run_201907231010_joined_entry_html;

--peaks kustutama
--1640
select *
FROM work.run_201907231010_joined_entry_html
WHERE id NOT IN
      (SELECT id
       FROM (
                select distinct on ( epi_id, analysis_name_raw, value, parameter_name, parameter_unit, reference_values_raw, effective_time) *
                from work.run_201907231010_joined_entry_html) as tbl);

/*
--peaks kustutama näited 5236 ,4583,5292,5290, AGA INFO LÄHEKS KAOTSI
select * from work.run_201907231010_joined_entry_html where id = '5236';
select * from work.run_201907231010_joined_entry_html
where epi_id = '37737811' and analysis_name_raw ='S-ASAT' and reference_values_raw = '-31';

select * from work.run_201907231010_joined_entry_html where id = '4583';
select * from work.run_201907231010_joined_entry_html
where epi_id = '44965266' and analysis_name_raw like'%seerumis' and reference_values_raw = '-500';
*/

-- 12563
select count(*)
from work.run_201907231010_joined_entry_html;

----------------------------------------------------------------------------------------
-- DUPLICATE ROWS WITH ALL INFORMATION
----------------------------------------------------------------------------------------

SELECT analysis_name, value, parameter_name, parameter_unit, reference_values_raw, effective_time
FROM (SELECT *,
             count(*)
             OVER
                 (PARTITION BY analysis_name, value, parameter_name, parameter_unit, reference_values_raw, effective_time) AS count
      FROM work.run_201907231010_joined_entry_html) tableWithCount
WHERE tableWithCount.count > 1
--epi_id = '44965266' and analysis_name_raw like'%seerumis' and reference_values_raw = '-500';
--epi_id = '39808887'-- and analysis_name_raw = '11.00 Glükoos täisveres (glükomeetriga)'


----------------------------------------------------------------------------------------
-- DELETING DUPLICATES
----------------------------------------------------------------------------------------
--For absolutely identical rows:
-- 12 215 on unikaalsed
SELECT DISTINCT *
FROM work.run_201907231010_joined_entry_html;

--For almost identical rows:
-- pmst unikaalsed 10 579
SELECT DISTINCT ON ( analysis_name, value, parameter_name, parameter_unit, reference_values_raw, effective_time) *
FROM work.run_201907231010_joined_entry_html;



----------------------------------------------------------------------------------------
-- COMBINING DUPLICATES INTO ONE ROW
----------------------------------------------------------------------------------------
select *
from work.run_201907231010_joined_entry_html
where epi_id = '44965266'
  and analysis_name_raw like '%seerumis'
  and reference_values_raw = '-500'

-- VÕIKS OLLA SEE VALMIS TABEL, aga ei võta arvesse, kui ühel PN null ja teisel PN WBC
SELECT analysis_name,
       value,
       parameter_name,
       parameter_unit,
       reference_values,
       effective_time,
       string_agg(distinct epi_id, ' / ')              AS epi_id,
       string_agg(distinct epi_type, ' / ')            AS epi_type,
       string_agg(distinct parse_type, ' / ')          AS parse_types,
    /*string_agg(original_analysis_entry_id, '/') AS original_analysis_entry_id,
    string_agg(analysis_id, '/') AS analysis_id,*/
       string_agg(distinct code_system, '/')           AS code_system,
       string_agg(distinct code_system_name, '/')      AS code_system_name,
       string_agg(distinct analysis_code_raw, '/')     AS analysis_code_raw,
       string_agg(distinct analysis_name_raw, '/')     AS analysis_name_raw,
       string_agg(distinct parameter_code_raw, '/')    AS parameter_code_raw,
       --string_agg(html_match_id, '/') AS html_match_id,
       string_agg(distinct entry_html_match_desc, '/') AS entry_html_match_desc,
       string_agg(distinct source, '/')                AS source,
       string_agg(distinct parse_type, '/')            AS parse_type,
       --string_agg(panel_id, '/') AS panel_id,
       string_agg(distinct analysis_substrate, '/')    AS analysis_substrate,
       --string_agg(entry_match_id, '/') AS entry_match_id,
       string_agg(distinct loinc_unit, '/')            AS loinc_unit,
       string_agg(distinct elabor_t_lyhend, '/')       AS elabor_t_lyhend,
       string_agg(distinct loinc_code, '/')            AS loinc_code
FROM work.run_201907231010_joined_entry_html
--where epi_id = '44965266' and analysis_name_raw like'%seerumis' and reference_values_raw = '-500'
--where epi_id='14052071' and parameter_name = 'RDW-SD'
where epi_id = '39808887'
  and analysis_name = '11.00 Glükoos täisveres (glükomeetriga)'
GROUP BY analysis_name, value, parameter_name, parameter_unit, reference_values, effective_time

select *
from work.run_201907231010_entry
where epi_id = '39808887'
  and value = '7,3'

/*
select * from work.run_201907231010_joined_entry_html
where analysis_name_raw ='Korduv glükoos kapillaarverest' and value='18,8'

SELECT
  analysis_name_raw, value, parameter_name, parameter_unit, reference_values, effective_time,
  string_agg(epi_id, ' / ') AS epi_id,
  string_agg(parse_type, ' / ') AS parse_types,
  string_agg(distinct effective_time_raw, '/') AS ef_time_raw
FROM work.run_201907231010_joined_entry_html
where analysis_name_raw ='Korduv glükoos kapillaarverest' and value='18,8'
GROUP BY
  analysis_name_raw, value, parameter_name, parameter_unit, reference_values, effective_time
 */


select parameter_name_raw, parameter_name, count(*)
from run_201908261300_analysis_entry_cleaned
where parameter_name_raw = parameter_name
group by parameter_name_raw, parameter_name
order by count desc;