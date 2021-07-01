set search_path to work;
------------------------------------------------------------------------------------------------------------------------
-- 1. Adding source column, removing unneccessary columns
------------------------------------------------------------------------------------------------------------------------
select count(*) from run_201909111308_analysis_entry_loinced_unique;--61975
select count(*) from run_201909111308_analysis_html_loinced_unique;--4023517
select * from run_201909111308_analysis_entry_loinced_unique limit 10;
select * from run_201909111308_analysis_html_loinced_unique limit 10;

ALTER TABLE run_201909111308_analysis_entry_loinced_unique
    ADD source varchar NOT NULL DEFAULT 'entry';

ALTER TABLE run_201909111308_analysis_html_loinced_unique
    ADD source varchar NOT NULL DEFAULT 'html';

ALTER TABLE run_201909111308_analysis_entry_loinced_unique
    drop column rn,
    drop column cleaned_value;

ALTER TABLE run_201909111308_analysis_html_loinced_unique
    drop column rn,
    drop column cleaned_value,
    drop column entry_match_id_html;

------------------------------------------------------------------------------------------------------------------------
-- 2. Creating UNresolved HTML and entry id tables
------------------------------------------------------------------------------------------------------------------------
-- all HTML id's where epi_id exists BOTH in entry and HTML
-- the rest (which are not in the table) are unique for HTML and therefore do not need resolving
drop table if exists run_201909111308_unresolved_html;
create table run_201909111308_unresolved_html as
select id
from run_201909111308_analysis_html_loinced_unique as html
where html.epi_id in (select epi_id from run_201909111308_analysis_entry_loinced_unique);


-- all ENTRY id's where epi_id exists BOTH in entry and HTML
-- the rest (which are not in the table) are unique for HTML and therefore do not need resolving
drop table if exists run_201909111308_unresolved_entry;
create table run_201909111308_unresolved_entry as
select id
from run_201909111308_analysis_entry_loinced_unique as entry
where entry.epi_id in (select epi_id from run_201909111308_analysis_html_loinced_unique);


select count(*) from work.run_201909021100_unresolved_html;
select count(*) from work.run_201909021100_unresolved_entry;


----------------------------------------------------------------------------
--1. Create initial matched table from PERFECT matches
-- E, A, P, PU, D, V, R are exactly the same
------------------------------------------------------------------------------
--  html.epi_id = entry.epi_id
--  html.analysis_name = entry.analysis_name
--  html.parameter_name = entry.parameter_name
--  html.parameter_unit = entry.parameter_unit
--  html.effective_time = entry.effective_time
--  html.value = entry.value
--  html.reference_values = entry.reference_values;
set search_path  to work;

CREATE INDEX idx_source_html
ON run_201909111308_analysis_html_loinced_unique(epi_id, analysis_name, parameter_name, parameter_unit, value, effective_time, reference_values);

CREATE idx_run_201909111308_analysis_html_loinced_unique_epiid_parametername_value_effectivetime
on run_201909111308_analysis_html_loinced_unique(epi_id, parameter_name, value, effective_time);

CREATE INDEX idx_source_entry
ON run_201909111308_analysis_entry_loinced_unique(epi_id, analysis_name, parameter_name, parameter_unit, value, effective_time, reference_values);

CREATE idx_run_201909111308_analysis_entry_loinced_unique_epiid_parametername_value_effectivetime
on run_201909111308_analysis_entry_loinced_unique(epi_id, parameter_name, value, effective_time);

DROP INDEX idx_source_entry cascade ; from run_201909111308_analysis_entry_loinced_unique;
DROP INDEX idx_source_html cascade ; ON run_201909111308_analysis_html_loinced_unique;

SELECT pid, query
FROM pg_stat_activity;

drop table if exists run_201909111308_matched;
create table run_201909111308_matched as
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
       --html.entry_match_id                                                                      as entry_match_id_html,
       html.entry_html_match_desc                                                               as entry_html_match_desc_html,
       html.effective_time_raw                                                                  as effective_time_raw_html,
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
from run_201909111308_analysis_html_loinced_unique as html
         join run_201909111308_analysis_entry_loinced_unique as entry
              on html.epi_id = entry.epi_id and
                 html.analysis_name = entry.analysis_name and
                 html.parameter_name = entry.parameter_name and
                 html.parameter_unit = entry.parameter_unit and
                 html.effective_time = entry.effective_time and
                 html.value = entry.value and
                 html.reference_values = entry.reference_values;

select count(*) from run_201909111308_matched; --3747

------------------------------------------------------------------------------------------------------------------------
-- 4. Add to matched table UNIQUE HTML and UNIQUE entry epi_ids
------------------------------------------------------------------------------------------------------------------------
--unique in HTML
insert
into run_201909161122_matched (row_nr_html,
                epi_id_html,
                epi_type_html,
                parse_type_html,
                panel_id_html,
                analysis_name_raw_html,
                parameter_name_raw_html,
                parameter_unit_raw_html,
                reference_values_raw_html,
                effective_time_raw_html,
                value_raw_html,
                analysis_substrate_raw_html,
                analysis_substrate_html,
                --entry_match_id_html,
                entry_html_match_desc_html,
                id_html,
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
(select *, 'only_html' as match_description
 from run_201909161122_analysis_html_loinced_unique
 where id not in (select id from run_201909161122_unresolved_html));

select count(*) from run_201909111308_matched; --3965608

select * from run_201909161122_matched;

select * from run_201909161122_analysis_html_loinced_unique;





-- unique in ENTRY
insert
into run_201909111308_matched (id_entry,
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
                analysis_name_entry,
                parameter_name_entry,
                effective_time_entry,
                value_entry,
                parameter_unit_from_suffix_entry,
                suffix_entry,
                value_type_entry,
                reference_values_entry,
                parameter_unit_entry,
                loinc_unit_ entry,
                elabor_t_lyhend_entry,
                loinc_code_entry,
                source_entry,
                match_description)
(select *, 'only_entry' as match_description
 from run_201909111308_analysis_entry_loinced_unique
 where id not in (select id from run_201909111308_unresolved_entry));

select count(*) from run_201909111308_matched; --3965853



----------------------------------------------------------------------------------
-- 5. Decrease unresolved
-- remove unique epi_ids and perfect matches
----------------------------------------------------------------------------------
DELETE
FROM run_201909111308_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909111308_matched);
-- unresolvedes 57983 rida

DELETE
FROM run_201909111308_unresolved_html
WHERE id in (SELECT id_html FROM run_201909111308_matched);
-- 57910


----------------------------------------------------------------------------------
-- We will continue work with UNRESOLVED rows
----------------------------------------------------------------------------------
-- 6. Matching by  V E D PN
----------------------------------------------------------------------------------
--  html.epi_id = entry.epi_id,
--  html.value = entry.value,
--  html.effective_time = entry.effective_time
--  html.paramter_name = entry.parameter_name

--5264, aga siin võib olla valesti ühendamisi
drop table if exists run_201909111308_matching_VEDP;
create table run_201909111308_matching_VEDP as
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
      -- html.entry_match_id              as entry_match_id_html,
       html.entry_html_match_desc       as entry_html_match_desc_html,
       html.effective_time_raw          as effective_time_raw_html,
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
from run_201909111308_analysis_html_loinced_unique as html
         join run_201909111308_analysis_entry_loinced_unique as entry
              on html.epi_id = entry.epi_id and
                 html.parameter_name = entry.parameter_name and
                 date(html.effective_time) = date(entry.effective_time) and
                 html.value = entry.value
where html.id in (select id from run_201909111308_unresolved_html)
   or entry.id in (select id from run_201909111308_unresolved_entry);



----------------------------------------------------------------------------------
-- 7. Matching by V E D PN U from previous table
----------------------------------------------------------------------------------
--  html.epi_id = entry.epi_id,
--  html.value = entry.value,
--  html.effective_time = entry.effective_time
--  html.paramter_name = entry.parameter_name
--  html.parameter_unit = entry.parameter_unit

drop table if exists run_201909111308_matching_VEDPU;
create table run_201909111308_matching_VEDPU as
    select *
    from run_201909111308_matching_VEDP
    where
        -- different cases for the units to match
        ((parameter_unit_html is not distinct from parameter_unit_entry) or --(so NULL = NULL would be TRUE)
        (parameter_unit_entry is NULL and parameter_unit_html is NOT NULL) or --one table does not have unit, the other does
        (parameter_unit_html is NULL and parameter_unit_entry is NOT NULL));

-----------------------------------------------------------------------------------------------------------------------
-- 8. ADDING V E D P U
-----------------------------------------------------------------------------------------------------------------------
-- Multiplicities: 1 entry ---- many html or 1 html ----- many entry
-- adding to matched table ONLY  the 1 ---- 1 matches
with vedpu_multip_entry as (
    -- 1 to many entry
    select id_entry
    from run_201909111308_matching_VEDPU
    group by id_entry
    having count(*) >= 2),
     -- 1 to many html
     vedpu_multip_html as (
         select id_html
         from run_201909111308_matching_VEDPU
         group by id_html
         having count(*) >= 2)
insert
into run_201909111308_matched
    (select *, 'epi_id, parameter_name, parameter_unit, effective_time, value'
     from run_201909111308_matching_VEDPU
    -- keep only 1--1 relationships
     where id_html not in (select id_html from vedpu_multip_html)
       and id_entry not in (select id_entry from vedpu_multip_entry));

-----------------------------------------------------------------------------------------------------------------------
-- 9. ADDING V E D P
-----------------------------------------------------------------------------------------------------------------------

with vedp_multip_entry as (
    -- 1 to many entry
    select id_entry
    from run_201909111308_matching_VEDP
    group by id_entry
    having count(*) >= 2),
     -- 1 to many html
     vedp_multip_html as (
         select id_html
         from run_201909111308_matching_VEDP
         group by id_html
         having count(*) >= 2)
insert
into run_201909111308_matched
    select *, 'epi_id, parameter_name, effective_time, value' from run_201909111308_matching_VEDP
        where -- not adding duplicate rows
              id_html not in (select id_html from run_201909111308_matched) and
              id_entry not in (select id_entry from run_201909111308_matched) and
              -- only 1 to 1 relationships will be added
              id_html not in (select id_html from vedp_multip_html) and
              id_entry not in (select id_entry from vedp_multip_entry);


-----------------------------------------------------------------------------------------------------------------------
-- 10. Decreasing UNresolved
-----------------------------------------------------------------------------------------------------------------------
DELETE
FROM run_201909111308_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909111308_matched);
--28289

DELETE
FROM run_201909111308_unresolved_html
WHERE id in (SELECT id_html FROM run_201909111308_matched);
--28216



-----------------------------------------------------------------------------------------------------------------------
-- 11. Detecting TIES
-- THEY WILL BE LEFT UNRESOLVED until PN cleaning is changed!!!
-----------------------------------------------------------------------------------------------------------------------
-- example: epi_id = '19289924' and value = '-0.4'; could be avoided if PN_raw is not cleaned too much
-- html|entry
--  a -- b
--    \/
--    /\
--  c -- d

drop table if exists run_201909111308_tie;
create table run_201909111308_tie as
with html_set as (
    select array_agg(distinct id_html) as id_html,
           epi_id_html,
           value_html,
           parameter_name_html,
           --parameter_unit_html,
           effective_time_html,
           count(*) as count_html
    from run_201909111308_matching_VEDP
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
         from run_201909111308_matching_VEDP
         group by epi_id_entry, value_entry, parameter_name_entry, effective_time_entry
)
select * from entry_set as e, html_set as h
where e.count_entry = h.count_html
    and e.count_entry > 1  -- we don't want 1 on 1 relationships
    and parameter_name_entry = parameter_name_html
    and value_entry = value_html
    and effective_time_entry = effective_time_html
    and epi_id_entry = epi_id_html;



------------------------------------------------------------------------------------------------------------------------
-- 12. Decrease unresolved by removing bow ties
------------------------------------------------------------------------------------------------------------------------
DELETE
FROM run_201909111308_unresolved_entry
WHERE id in (SELECT unnest(id_entry) FROM run_201909111308_bow_tie);
-- 28230

DELETE
FROM run_201909111308_unresolved_html
WHERE id in (SELECT unnest(id_html) FROM run_201909111308_bow_tie);
--28157



----------------------------------------------------------------------------------
-- 13. Matching by E P D  where values missing from both tables
----------------------------------------------------------------------------------
-- value_html = null ja value_entry = null

drop table if exists run_201909111308_matching_EPD;
create table run_201909111308_matching_EPD as
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
       --html.entry_match_id              as entry_match_id_html,
       html.entry_html_match_desc       as entry_html_match_desc_html,
       html.effective_time_raw          as effective_time_raw_html,
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
    from run_201909111308_analysis_html_loinced_unique as html
             join
         run_201909111308_analysis_entry_loinced_unique as entry
         on html.epi_id = entry.epi_id and
            html.parameter_name = entry.parameter_name and
            date(html.effective_time) = date(entry.effective_time)
    --only match unresolved rows
    where (html.id in (select id from run_201909111308_unresolved_html) or
           entry.id in (select id from run_201909111308_unresolved_entry))
)
select * from epd_matched
where value_html is null and value_entry is null;

------------------------------------------------------------------------------------------------------------------------
-- Mutliplicity
-- example
-- id_html, id_entry,
-- 4060022, 9939,
-- 4059982, 9939,

-- 1 html many entry
with edp_multip_entry as (
    select id_entry
    from run_201909111308_matching_EPD
    group by id_entry
    having count(*) >= 2),
     -- 1 entry many HTML
     edp_multip_html as (
         select id_html
         from run_201909111308_matching_EPD
         group by id_html
         having count(*) >= 2)
--add 1--1 rows to matched
insert into run_201909111308_matched
select *, 'epi_id, parameter_name, effective_time, value_html = NULL, value_entry = NULL' as match_descrition
from run_201909111308_matching_EPD
where id_entry not in (select id_entry from edp_multip_entry)
  and id_html not in (select id_html from edp_multip_html);



---------------------------------------------------------------------------------------------
-- 14. Decrease unresolved by E P D
----------------------------------------------------------------------------------------------
DELETE
FROM run_201909111308_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909111308_matched);
--27999

DELETE
FROM run_201909111308_unresolved_html
WHERE id in (SELECT id_html FROM run_201909111308_matched);
--27926


------------------------------------------------------------------------------------
--15. matchimine V E P (missing date from either of thetable)
------------------------------------------------------------------------------------
drop table if exists run_201909111308_matching_vep;
create table run_201909111308_matching_vep as
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
           --html.entry_match_id              as entry_match_id_html,
           html.entry_html_match_desc       as entry_html_match_desc_html,
           html.effective_time_raw          as effective_time_raw_html,
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
    from run_201909111308_analysis_html_loinced_unique as html
             join
         run_201909111308_analysis_entry_loinced_unique as entry
         on html.epi_id = entry.epi_id and
            html.parameter_name = entry.parameter_name and
            html.value = entry.value
         --unresolved rows
    where (html.id in (select id from run_201909111308_unresolved_html) or
           entry.id in (select id from run_201909111308_unresolved_entry))
)
select *
from vep_matched
where effective_time_html is null
   or effective_time_entry is null;
--45

-----------------------------------------------------------------------------------------------------------------------
-- Multiplicities
-- example: html = 2962412, entry = 66119, 66127

-- 1 HTML many entry
with vep_multip_entry as (
    select id_entry
    from run_201909111308_matching_vep
    group by id_entry
    having count(*) >= 2),
     -- 1 entry many HTML
     vep_multip_html as (
         select id_html
         from run_201909111308_matching_vep
         group by id_html
         having count(*) >= 2)
-- 1 to 1 relationships added to matched table
insert into run_201909111308_matched
select *, 'epi_id, parameter_name, value, effective_time_html = NULL or effective_time_entry = NULL' as match_descrition
from run_201909111308_matching_vep
where id_entry not in (select id_entry from vep_multip_entry)
  and id_html not in (select id_html from vep_multip_html);



---------------------------------------------------------------------------------------------
-- 16. Decrease unresolved by V E P
----------------------------------------------------------------------------------------------
DELETE
FROM run_201909111308_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909111308_matched);
--27956

DELETE
FROM run_201909111308_unresolved_html
WHERE id in (SELECT id_html FROM run_201909111308_matched);
--27883




------------------------------------------------------------------------------------
-- 17. Matching V E D (parameter_name is wrong), either pn_entry = NULL or pn_html = NULL
------------------------------------------------------------------------------------
-- mainly cases where in HTML PN is for some reason under AN

drop table if exists run_201909111308_matching_ved;
create table run_201909111308_matching_ved as
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
       --html.entry_match_id              as entry_match_id_html,
       html.entry_html_match_desc       as entry_html_match_desc_html,
       html.effective_time_raw          as effective_time_raw_html,
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
    from run_201909111308_analysis_html_loinced_unique as html
             join
         run_201909111308_analysis_entry_loinced_unique as entry
         on html.epi_id = entry.epi_id and
            date(html.effective_time) = date(entry.effective_time) and
            html.value = entry.value
    -- trying to resolve only unresolved rows
    where (html.id in (select * from run_201909111308_unresolved_html) or
           entry.id in (select * from run_201909111308_unresolved_entry))
)
select *
from ved_matched
where parameter_name_entry is null
   or parameter_name_html is null;
--137


-----------------------------------------------------------------------------------------------------------
-- Multiplicities V E D
------------------------------------------------------------------------------------------------------------
-- example:  one id_entry = 8756, two id_html = 4910093, 4910073
-- MAINLY parse_type = 2.2 (PN under AN)


-- 1 HTML many entry
with ved_multip_entry as (
    select id_entry
    from run_201909111308_matching_ved
    group by id_entry
    having count(*) >= 2),
     -- 1 entry many HTML
     ved_multip_html as (
         select id_html
         from run_201909111308_matching_ved
         group by id_html
         having count(*) >= 2)
-- 1 on 1 relationship matches
insert
into run_201909111308_matched
    (select distinct *, 'epi_id, effective_time, value, parameter_name_entry = NULL or parameter_name_html = NULL' as match_description
     from run_201909111308_matching_ved
     where id_entry not in (select id_entry from ved_multip_entry)
       and id_html not in (select id_html from ved_multip_html));



---------------------------------------------------------------------------------------------
-- 18. Decrease unresolved by V E D
----------------------------------------------------------------------------------------------
DELETE
FROM run_201909111308_unresolved_entry
WHERE id in (SELECT id_entry FROM run_201909111308_matched);
--13917

DELETE
FROM run_201909111308_unresolved_html
WHERE id in (SELECT id_html FROM run_201909111308_matched);
--13844

select count(*) from run_201909111308_matched; --4009860
select count(*) from run_201909111308_unresolved_html; --13844
select count(*) from run_201909111308_unresolved_entry; --13917
select count(*) from run_201909111308_analysis_entry_loinced_unique; --61975
select count(*) from run_201909111308_analysis_html_loinced_unique; ---4023517

-- kuskil 50 000 matchitud rida
select count(*) from run_201909021100_matched; --3999091
select count(*) from run_201909021100_unresolved_html; --24610
select count(*) from run_201909021100_unresolved_entry; --24682

select distinct entry_html_match_desc from work.run_201909161122_analysis_html_loinced_unique;


---------------------------------------------------------------------------------------------
-- 19. Decrease unresolved by V E D -- unmatched to mathched table with description that the row was not matched
----------------------------------------------------------------------------------------------
insert into {matched} (row_nr_html,
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
                       --entry_match_id_html,
                       entry_html_match_desc_html,
                       id_html,
                       effective_time_raw_html,
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
select *, 'unresolved_html'
from run_201909021100_analysis_html_loinced_unique
where id in (select id from run_201909021100_unresolved_html);

insert into {matched} (id_entry,
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
select *, 'unresolved_entry'
from run_201909021100_analysis_entry_loinced_unique
where id in (select id from run_201909021100_unresolved_entry);


set search_path to hwisc_epi;
drop function original_microset_fixed.exist_table