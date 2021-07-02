set search_path to work;

select count(*) from run_201909021100_matched;
select distinct match_description from run_201909181139_matched;

-- Kokku 8 erinevat tüüpi matchimist
/*
*only_html
*only_entry
*analysis_name, parameter_name, parameter_unit, effective_time, value, reference_values"
*epi_id, parameter_name, parameter_unit, effective_time, value"
*epi_id, parameter_name, effective_time, value_html = NULL and value_entry = NULL"

*unresolved_entry
*unresolved_html
*epi_id, parameter_name, value, effective_time_html = NULL or effective_time_entry = NULL"
*/

-----------------------------------------------------------------------------------------------------------------------
-- creating the final table
-----------------------------------------------------------------------------------------------------------------------
create table run_201909191153_analysis_entry_html
(
    epi_id                   varchar,
    analysis_name            varchar,
    parameter_name           varchar,
    parameter_unit           varchar,
    effective_time           timestamp without time zone,
    value                    varchar,
    reference_values         varchar,
    analysis_substrate       varchar,
    code_system_entry        varchar,
    code_system_name_entry   varchar,
    analysis_code_raw_entry  varchar,
    parameter_code_raw_entry varchar,
    analysis_id_entry        integer,
    loinc_code               varchar,
    elabor_t_lyhend          varchar,
    analysis_name_raw        varchar,
    parameter_name_raw       varchar,
    parameter_unit_raw       varchar,
    effective_time_raw       varchar,
    value_raw                varchar,
    reference_values_raw     varchar,
    id_entry                 integer,
    id_html                  integer,
    match_description        varchar
);

select * from run_201909191153_analysis_html_entry;
-----------------------------------------------------------------------------------------------------------------------
-- ONLY HTML 3 961 861 rida
-----------------------------------------------------------------------------------------------------------------------
insert into run_201909191153_analysis_entry_html values ('a1','a','a','a',to_date('20170103','YYYYMMDD'),'a','a','a','a','a10','a','a',1,'a','a15','a','a','a','a','a20','a','a','a','a24');
select * from run_201909191153_analysis_entry_html;


insert into run_201909191153_analysis_entry_html (
select epi_id_html               as epi_id,
       analysis_name_html        as analysis_name,
       parameter_name_html       as parameter_name,
       parameter_unit_html       as parameter_unit,
       effective_time_html       as effective_time,
       value_html                as value,
       reference_values_html     as reference_values,
       analysis_substrate_html   as analysis_substrate, --exists only in HTML
       code_system_entry,        -- exist only in ENTRY
       code_system_name_entry,   -- exist only in ENTRY
       analysis_code_raw_entry,  -- exist only in ENTRY
       parameter_code_raw_entry, -- exist only in ENTRY
       analysis_id_entry,        -- exist only in ENTRY
       loinc_code_html           as loinc_code,
       elabor_t_lyhend_html      as elabor_t_lyhend,
       analysis_name_raw_html    as analysis_name_raw,
       parameter_name_raw_html   as parameter_name_raw,
       parameter_unit_raw_html   as parameter_unit_raw,
       effective_time_raw_html   as effective_time_raw,
       value_raw_html            as value_raw,
       reference_values_raw_html as reference_values_raw,
       id_entry                  as id_entry,
       id_html                   as id_html,
       match_description
from run_201909191153_matched
where match_description = 'only_html');

select count(*) from run_201909021100_analysis_html_entry; --3961861

select * from run_201909191153_analysis_entry_html;

-----------------------------------------------------------------------------------------------------------------------
-- ONLY ENTRY 245 rida
-----------------------------------------------------------------------------------------------------------------------
insert into run_201909021100_analysis_html_entry (
select epi_id_entry               as epi_id,
       analysis_name_entry        as analysis_name,
       parameter_name_entry       as parameter_name,
       parameter_unit_entry       as parameter_unit,
       effective_time_entry       as effective_time,
       value_entry                as value,
       reference_values_entry     as reference_values,
       analysis_substrate_html   as analysis_substrate, --exists only in HTML
       code_system_entry,        -- exist only in ENTRY
       code_system_name_entry,   -- exist only in ENTRY
       analysis_code_raw_entry,  -- exist only in ENTRY
       parameter_code_raw_entry, -- exist only in ENTRY
       analysis_id_entry,        -- exist only in ENTRY
       loinc_code_entry           as loinc_code,
       elabor_t_lyhend_entry      as elabor_t_lyhend,
       analysis_name_raw_entry    as analysis_name_raw,
       parameter_name_raw_entry   as parameter_name_raw,
       parameter_unit_raw_entry   as parameter_unit_raw,
       effective_time_raw_entry   as effective_time_raw,
       value_raw_entry            as value_raw,
       reference_values_raw_entry as reference_values_raw,
       id_entry                   as id_entry,
       id_html                    as id_html,
       match_description
from run_201909021100_matched
where match_description = 'only_entry');

select count(*) from run_201909021100_analysis_html_entry; --3962106

-----------------------------------------------------------------------------------------------------------------------
-- E, AN, P, PU, D, V, R - 5953 rida
-- analysis_name, parameter_name, parameter_unit, effective_time, value, reference_values
-----------------------------------------------------------------------------------------------------------------------
-- all the columns are same so does not matter if we use entry or html columns
insert into run_201909021100_analysis_html_entry (
select epi_id_html             as epi_id,
       analysis_name_html      as analysis_name,
       parameter_name_html     as parameter_name,
       parameter_unit_html     as parameter_unit,
       effective_time_html     as effective_time,
       value_html              as value,
       reference_values_html   as reference_values,
       analysis_substrate_html   as analysis_substrate, --exists only in HTML
       code_system_entry,        -- exist only in ENTRY
       code_system_name_entry,   -- exist only in ENTRY
       analysis_code_raw_entry,  -- exist only in ENTRY
       parameter_code_raw_entry, -- exist only in ENTRY
       analysis_id_entry,        -- exist only in ENTRY
       loinc_code_html           as loinc_code,
       elabor_t_lyhend_html      as elabor_t_lyhend,
       analysis_name_raw_html    as analysis_name_raw,
       parameter_name_raw_html   as parameter_name_raw,
       parameter_unit_raw_html   as parameter_unit_raw,
       effective_time_raw_html   as effective_time_raw,
       value_raw_html            as value_raw,
       reference_values_raw_html as reference_values_raw,
       id_entry                  as id_entry,
       id_html                   as id_html,
       match_description
from run_201909021100_matched
where match_description = 'analysis_name, parameter_name, parameter_unit, effective_time, value, reference_values');

select distinct match_description from run_201909191153_analysis_entry_html; --3968059
-----------------------------------------------------------------------------------------------------------------------
-- E, P, PU, D, V - 30682 rida
-- choosing appropriate AN and R
-- epi_id, parameter_name, parameter_unit, effective_time, value
-----------------------------------------------------------------------------------------------------------------------

insert into run_201909021100_analysis_html_entry (
    select epi_id_html             as epi_id,
           --analysis_name
           CASE
               --one is empty, the other one is not
               WHEN analysis_name_html is not null and
                    (analysis_name_entry is null or analysis_name_entry ~ '<ANONYM(.+)/>')
                   THEN analysis_name_html
               WHEN analysis_name_entry is not null and
                    (analysis_name_html is null or analysis_name_html ~ '<ANONYM(.+)/>')
                   THEN analysis_name_entry
               -- both are not null but they are different -> prefer HTML
               -- both are not null but same -> does not matter wheater to take HMTL or entry
               ELSE analysis_name_html
               END                 AS analysis_name,

           parameter_name_html     as parameter_name,     --same in both entry and HTML
           parameter_unit_html     as parameter_unit,     --same in both entry and HTML
           effective_time_html     as effective_time,     --same in both entry and HTML
           value_html              as value,              --same in both entry and HTML

           -- reference_values
           CASE
               WHEN reference_values_html is not null and
                    (reference_values_entry is null or upper(reference_values_entry) ~ '<ANONYM(.*)/>')
                   THEN reference_values_html
               WHEN reference_values_entry is not null and
                    (reference_values_html is null or upper(reference_values_html) ~ '<ANONYM(.*)/>')
                   THEN reference_values_entry
               ELSE reference_values_html
               END                 AS reference_values,

           analysis_substrate_html as analysis_substrate, --exists only in HTML
           code_system_entry,                             -- exist only in ENTRY
           code_system_name_entry,                        -- exist only in ENTRY
           analysis_code_raw_entry,                       -- exist only in ENTRY
           parameter_code_raw_entry,                      -- exist only in ENTRY
           analysis_id_entry,                             -- exist only in ENTRY
           loinc_code_html         as loinc_code,
           elabor_t_lyhend_html    as elabor_t_lyhend,

           --analysis_name_raw
           CASE
               --one is empty, the other one is not
               WHEN analysis_name_raw_html is not null and
                    (analysis_name_raw_entry is null or upper(analysis_name_raw_entry) ~ '<ANONYM(.+)/>')
                   THEN analysis_name_raw_html
               WHEN analysis_name_raw_entry is not null and
                    (analysis_name_raw_html is null or upper(analysis_name_raw_html) ~ '<ANONYM(.+)/>')
                   THEN analysis_name_raw_entry
               -- both are not null but they are different -> prefer HTML
               -- both are not null but same -> does not matter wheater to take HMTL or entry
               ELSE analysis_name_raw_html
               END                 AS analysis_name_raw,

           parameter_name_raw_html as parameter_name_raw,
           parameter_unit_raw_html as parameter_unit_raw,
           effective_time_raw_html as effective_time_raw,
           value_raw_html          as value_raw,

           -- reference_values_raw
           CASE
               --one is empty, the other one is not
               WHEN reference_values_raw_html is not null and
                    (reference_values_raw_entry is null or upper(reference_values_raw_entry) ~ '<ANONYM(.*)/>')
                   THEN reference_values_raw_html
               WHEN reference_values_raw_entry is not null and
                    (reference_values_raw_html is null or upper(reference_values_raw_html) ~ '<ANONYM(.*)/>')
                   THEN reference_values_raw_entry
               -- both are not null but they are different -> prefer HTML
               -- both are not null but same -> does not matter weather to take HMTL or entry
               ELSE reference_values_raw_html
               END                 as reference_values_raw,
           id_entry                as id_entry,
           id_html                 as id_html,
           match_description
    from run_201909021100_matched
    where match_description = 'epi_id, parameter_name, parameter_unit, effective_time, value');


select count(*) from run_201909021100_analysis_html_entry; --3998741

-----------------------------------------------------------------------------------------------------------------------
-- E, P, D - 190
-- V is NULL
-- choosing appropriate AN, R, PU and V
-- epi_id, parameter_name, effective_time, value_html = NULL and value_entry = NULL"
-----------------------------------------------------------------------------------------------------------------------
select distinct match_description from run_201909191153_matched;

insert into run_201909021100_analysis_html_entry (
    select epi_id_html             as epi_id,             --same in both

           --analysis_name
           CASE
               WHEN analysis_name_html is not null and
                    (analysis_name_entry is null or upper(analysis_name_entry) ~ '<ANONYM(.+)/>')
                   THEN analysis_name_html
               WHEN analysis_name_entry is not null and
                    (analysis_name_html is null or upper(analysis_name_html) ~ '<ANONYM(.+)/>')
                   THEN analysis_name_entry
               ELSE analysis_name_html
               END                 AS analysis_name,

           parameter_name_html     as parameter_name,     --same in both

           CASE
               WHEN parameter_unit_html is not null and parameter_unit_entry is null THEN parameter_unit_html
               WHEN parameter_unit_entry is not null and parameter_unit_html is null THEN parameter_unit_entry
               ELSE parameter_unit_html
               END                 AS parameter_unit,

           effective_time_html     as effective_time,     --same in both
           NULL                    as value,              --same in both

           CASE
               WHEN reference_values_html is not null and
                    (reference_values_entry is null or upper(reference_values_entry) ~ '<ANONYM(.*)/>')
                   THEN reference_values_html
               WHEN reference_values_entry is not null and
                    (reference_values_html is null or upper(reference_values_html) ~ '<ANONYM(.*)/>')
                   THEN reference_values_entry
               ELSE reference_values_html
               END                 AS reference_values,

           analysis_substrate_html as analysis_substrate, --exists only in HTML
           code_system_entry,                             -- exist only in ENTRY
           code_system_name_entry,                        -- exist only in ENTRY
           analysis_code_raw_entry,                       -- exist only in ENTRY
           parameter_code_raw_entry,                      -- exist only in ENTRY
           analysis_id_entry,                             -- exist only in ENTRY
           loinc_code_html         as loinc_code,
           elabor_t_lyhend_html    as elabor_t_lyhend,

           --analysis_name_raw
           CASE
               WHEN analysis_name_raw_html is not null and
                    (analysis_name_raw_entry is null or upper(analysis_name_raw_entry) ~ '<ANONYM(.+)/>')
                   THEN analysis_name_raw_html
               WHEN analysis_name_raw_entry is not null and
                    (analysis_name_raw_html is null or upper(analysis_name_raw_html) ~ '<ANONYM(.+)/>')
                   THEN analysis_name_raw_entry
               ELSE analysis_name_raw_html
               END                 AS analysis_name_raw,

           parameter_name_raw_html as parameter_name_raw,

           --parameter_unit_raw
           CASE
               WHEN parameter_unit_raw_html is not null and parameter_unit_raw_entry is null
                   THEN parameter_unit_raw_html
               WHEN parameter_unit_raw_entry is not null and parameter_unit_raw_html is null
                   THEN parameter_unit_raw_entry
               ELSE parameter_unit_raw_html
               END                 as parameter_unit_raw,

           effective_time_raw_html as effective_time_raw,
           NULL                    as value_raw,

           -- reference_values_raw
           CASE
               WHEN reference_values_raw_html is not null and
                    (reference_values_raw_entry is null or upper(reference_values_raw_entry) ~ '<ANONYM(.*)/>')
                   THEN reference_values_raw_html
               WHEN reference_values_raw_entry is not null and
                    (reference_values_raw_html is null or upper(reference_values_raw_html) ~ '<ANONYM(.*)/>')
                   THEN reference_values_raw_entry
               ELSE reference_values_raw_html
               END                 as reference_values_raw,
           id_entry                as id_entry,
           id_html                 as id_html,
           match_description
    from run_201909021100_matched
    where match_description = 'epi_id, parameter_name, effective_time, value_html = NULL and value_entry = NULL');


select count(*) from run_201909021100_analysis_html_entry; --3998931

-----------------------------------------------------------------------------------------------------------------------
-- E, P, V - 43
-- choosing appropriate AN, R, PU and D
-- epi_id, parameter_name, value, effective_time_html = NULL or effective_time_entry = NULL
-----------------------------------------------------------------------------------------------------------------------

--one is empty, the other one is not
-- both entry and html are not null but they are different -> prefer HTML
-- both entry and html are not null but same -> does not matter weather to take HMTL or entry

insert into run_201909021100_analysis_html_entry (
select epi_id_html             as epi_id,         --same in both

       --analysis_name
       CASE
           WHEN analysis_name_html is not null and
                (analysis_name_entry is null or upper(analysis_name_entry) ~ '<ANONYM(.+)/>')
               THEN analysis_name_html
           WHEN analysis_name_entry is not null and
                (analysis_name_html is null or upper(analysis_name_html) ~ '<ANONYM(.+)/>')
               THEN analysis_name_entry
           ELSE analysis_name_html
           END                 AS analysis_name,

       parameter_name_html     as parameter_name, --same in both

       CASE
           WHEN parameter_unit_html is not null and parameter_unit_entry is null THEN parameter_unit_html
           WHEN parameter_unit_entry is not null and parameter_unit_html is null THEN parameter_unit_entry
           ELSE parameter_unit_html
           END                 AS parameter_unit,

       CASE
           WHEN effective_time_html is not null and effective_time_entry is null THEN effective_time_html
           WHEN effective_time_entry is not null and effective_time_html is null THEN effective_time_entry
           ELSE effective_time_html
           END                 AS effective_time,
       value_html              as value,          --same in both

       CASE
           WHEN reference_values_html is not null and
                (reference_values_entry is null or upper(reference_values_entry) ~ '<ANONYM(.*)/>')
               THEN reference_values_html
           WHEN reference_values_entry is not null and
                (reference_values_html is null or upper(reference_values_html) ~ '<ANONYM(.*)/>')
               THEN reference_values_entry
           ELSE reference_values_html
           END                 AS reference_values,

       analysis_substrate_html   as analysis_substrate, --exists only in HTML
       code_system_entry,                         -- exist only in ENTRY
       code_system_name_entry,                    -- exist only in ENTRY
       analysis_code_raw_entry,                   -- exist only in ENTRY
       parameter_code_raw_entry,                  -- exist only in ENTRY
       analysis_id_entry,                         -- exist only in ENTRY
       loinc_code_html         as loinc_code,
       elabor_t_lyhend_html    as elabor_t_lyhend,

       --analysis_name_raw
       CASE
           WHEN analysis_name_raw_html is not null and
                (analysis_name_raw_entry is null or upper(analysis_name_raw_entry) ~ '<ANONYM(.+)/>')
               THEN analysis_name_raw_html
           WHEN analysis_name_raw_entry is not null and
                (analysis_name_raw_html is null or upper(analysis_name_raw_html) ~ '<ANONYM(.+)/>')
               THEN analysis_name_raw_entry
           ELSE analysis_name_raw_html
           END                 AS analysis_name_raw,

       parameter_name_raw_html as parameter_name_raw,

       --parameter_unit_raw
       CASE
           WHEN parameter_unit_raw_html is not null and parameter_unit_raw_entry is null THEN parameter_unit_raw_html
           WHEN parameter_unit_raw_entry is not null and parameter_unit_raw_html is null THEN parameter_unit_raw_entry
           ELSE parameter_unit_raw_html
           END                 as parameter_unit_raw,

       --effective_time_Raw
       CASE
           WHEN effective_time_raw_html is not null and effective_time_raw_entry is null THEN effective_time_raw_html
           WHEN effective_time_raw_entry is not null and effective_time_raw_html is null THEN effective_time_raw_entry
           ELSE effective_time_raw_html
           END                 as effective_time_raw,

       value_raw_html          as value_raw,      --same in both

       CASE
           WHEN reference_values_raw_html is not null and
                (reference_values_raw_entry is null or upper(reference_values_raw_entry) ~ '<ANONYM(.*)/>')
               THEN reference_values_raw_html
           WHEN reference_values_raw_entry is not null and
                (reference_values_raw_html is null or upper(reference_values_raw_html) ~ '<ANONYM(.*)/>')
               THEN reference_values_raw_entry
           ELSE reference_values_raw_html
           END                 as reference_values_raw,

       id_entry                as id_entry,
       id_html                 as id_html,
       match_description
from run_201909021100_matched
where match_description = 'epi_id, parameter_name, value, effective_time_html = NULL or effective_time_entry = NULL');

select count(*) from run_201909021100_analysis_html_entry; --3998974

-----------------------------------------------------------------------------------------------------------------------
-- Unresolved HTML 24 727 rida
-----------------------------------------------------------------------------------------------------------------------

insert into run_201909021100_analysis_html_entry (
select epi_id_html               as epi_id,
       analysis_name_html        as analysis_name,
       parameter_name_html       as parameter_name,
       parameter_unit_html       as parameter_unit,
       effective_time_html       as effective_time,
       value_html                as value,
       reference_values_html     as reference_values,
       analysis_substrate_html   as analysis_substrate, --exists only in HTML
       code_system_entry,                             -- exist only in ENTRY
       code_system_name_entry,                        -- exist only in ENTRY
       analysis_code_raw_entry,                       -- exist only in ENTRY
       parameter_code_raw_entry,                      -- exist only in ENTRY
       analysis_id_entry,                             -- exist only in ENTRY
       loinc_code_html           as loinc_code,
       elabor_t_lyhend_html      as elabor_t_lyhend,
       analysis_name_raw_html    as analysis_name_raw,
       parameter_name_raw_html   as parameter_name_raw,
       parameter_unit_raw_html   as parameter_unit_raw,
       effective_time_raw_html   as effective_time_raw,
       value_raw_html            as value_raw,
       reference_values_raw_html as reference_values_raw,
       id_entry                  as id_entry,
       id_html                   as id_html,
       match_description
from run_201909021100_matched
where match_description = 'unresolved_html');


select count(*) from run_201909021100_analysis_html_entry; --4023701


-----------------------------------------------------------------------------------------------------------------------
-- Unresolved ENTRY 24 799 rida
-----------------------------------------------------------------------------------------------------------------------

insert into run_201909021100_analysis_html_entry (
select epi_id_entry              as epi_id,
       analysis_name_entry       as analysis_name,
       parameter_name_entry      as parameter_name,
       parameter_unit_entry      as parameter_unit,
       effective_time_entry      as effective_time,
       value_entry               as value,
       reference_values_entry    as reference_values,
       analysis_substrate_html   as analysis_substrate, --exists only in HTML
       code_system_entry,        -- exist only in ENTRY
       code_system_name_entry,   -- exist only in ENTRY
       analysis_code_raw_entry,  -- exist only in ENTRY
       parameter_code_raw_entry, -- exist only in ENTRY
       analysis_id_entry,        -- exist only in ENTRY
       loinc_code_html           as loinc_code,
       elabor_t_lyhend_html      as elabor_t_lyhend,
       analysis_name_raw_html    as analysis_name_raw,
       parameter_name_raw_html   as parameter_name_raw,
       parameter_unit_raw_html   as parameter_unit_raw,
       effective_time_raw_html   as effective_time_raw,
       value_raw_html            as value_raw,
       reference_values_raw_html as reference_values_raw,
       id_entry                  as id_entry,
       id_html                   as id_html,
       match_description
from run_201909021100_matched
where match_description = 'unresolved_entry');

select count(*) from run_201909021100_analysis_html_entry; --4048500

-----------------------------
-- NB!!!! lipse pole!!
-----------------------------
select count(*) from run_201909021100_matched;

select distinct match_description from run_201909191153_analysis_entry_html;

SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'work'
   AND table_name   = 'run_201909021100_analysis_html_entry';

select * from run_201909231229_analysis_cleaned;