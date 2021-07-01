set search_path  to work;
---------------------------------------------------------------
-- tabei loomine, mida cleaninda
-- sisaldab ainult _raw veerge
---------------------------------------------------------------

drop table if exists work.run_201903041255_analysis_html_nano;
set role egcut_epi_work_create;
create table work.run_201903041255_analysis_html_nano as
select epi_id,  analysis_name_raw, parameter_name_raw, parameter_unit_raw, value_raw, reference_values_raw, effective_time_raw, analysis_substrate_raw
from work.runfull201903041255_analysis_html_new
limit 1000000;

select count(*) from work.run_201903041255_analysis_html_nano;
select count(*) from work.possible_units;
select * from work.run_201903041255_analysis_html_pico2 limit 1;
------------------------------------------------------------------------------------------------------
-- NB!! html parsimine peaks jätam effective_time_raw VARCHAR ja cleanimine muutma selle TIMESTAMPiks
-- siis saab entry'l ja html-l samu puhastus funktsioone kasutada
------------------------------------------------------------------------------------------------------

ALTER TABLE work.run_201903041255_analysis_html_nano
ALTER COLUMN effective_time_raw TYPE VARCHAR;

-------------------------------------------------------------
-- LUIGI töövoo queryd

/*ALTER TABLE work.run_201903041255_analysis_html_pico1
                                    DROP COLUMN IF EXISTS value,
                                    DROP COLUMN IF EXISTS parameter_name,
                                    DROP COLUMN IF EXISTS analysis_name,
                                    DROP COLUMN IF EXISTS reference_values,
                                    DROP COLUMN IF EXISTS effective_time;

select * from work.run_201903041255_analysis_html_pico1 limit 1;
*/
---------------------------------------------------------------
-- CLEANING

SELECT pid, query FROM pg_stat_activity;
SELECT pg_terminate_backend(6180),pg_terminate_backend(26232),pg_terminate_backend(4197),pg_terminate_backend(10955),
       pg_terminate_backend(19130),pg_terminate_backend(25712);,pg_terminate_backend(23956),pg_terminate_backend(23956),;

drop table if exists work.possible_units;
select * from work.possible_units limit 1;

SET search_path TO work;
set role egcut_epi_work_create;

create table work.run_201908071459_html_cleaned as
with value_type as
    (
        -- determine value type (float, ratio etc)
        select *, check_types(value_raw) as value_type
        from work.run_201903041255_analysis_html_nano
        limit 20
    )
select *,
          clean_analysis_name(analysis_name_raw) as analysis_name,
          clean_parameter_name(parameter_name_raw) as parameter_name,
          clean_effective_time(effective_time_raw) as effective_time
          --clean_value(value_raw, value_type) as value
          --clean_reference_value(reference_values_raw) as reference_values
from value_type; --work.run_201903041255_analysis_html_pico2; --value_type;


set role egcut_epi_work_create;

create table work.run_201908071634_html_cleaned (
    epi_id varchar,
    analysis_name_raw varchar,
    parameter_name_raw varchar,
    parameter_unit_raw varchar,
    value_raw varchar,
    reference_values_raw varchar,
    effective_time_raw varchar,
    analysis_substrate_raw varchar,
    value_type varchar,
    analysis_name varchar,
    parameter_name varchar,
    effective_time varchar,
    value varchar,
    reference_values varchar);

insert into  work.run_201908071634_html_cleaned (
--create table work.run_201908071613_html_cleaned as
    select *,
           clean_analysis_name(analysis_name_raw)      as analysis_name,
           clean_parameter_name(parameter_name_raw)    as parameter_name,
           clean_effective_time(effective_time_raw)    as effective_time,
           clean_value(value_raw, value_type)          as value,
           clean_reference_value(reference_values_raw) as reference_values
    from ( -- determine value type (float, ratio etc)
             select *, check_types(value_raw) as value_type
             from work.run_201903041255_analysis_html_nano
         ) as tbl
);

select count(*) from work.run_201908081103_analysis_entry





----------------------------------------------------------------------------------------

-- kaduma läinud effective_time_raw veeru tagasi saamine
ALTER TABLE work.runfull201903041255_analysis_html
ADD COLUMN effective_time_raw VARCHAR;

update work.runfull201903041255_analysis_html as h
set effective_time_raw = n.effective_time_raw
from work.runfull201902260943_analysis_html as n
where h.epi_id = n.epi_id and
   h.row_nr = n.row_nr and
   h.panel_id = n.panel_id;

      /*h.epi_id = n.epi_id and
   h.analysis_name_raw = n.analysis_name_raw and
   h.parameter_name_raw = n.parameter_name_raw and
   h.parameter_unit_raw = n.parameter_unit_raw and
   h.value_raw = n.value_raw;*/



select h.*, n.effective_time_raw from work.runfull201903041255_analysis_html as h
left join work.runfull201903041255_analysis_html_new as n
on h.epi_id = n.epi_id and
   h.row_nr = n.row_nr and
   h.panel_id = n.panel_id;

   /*
   h.analysis_name_raw = n.analysis_name_raw and
   h.parameter_name_raw = n.parameter_name_raw and
   h.parameter_unit_raw = n.parameter_unit_raw and
   h.value_raw = n.value_raw;
*/

select * from work.runfull201903041255_analysis_html limit 1000;
select * from work.runfull201902260943_analysis_html limit 1000;

select count(*) from work.runfull201903041255_analysis_html
where effective_time_raw is not null;--2351169
select count(*) from work.runfull201902260943_analysis_html
where effective_time_raw is not null; --4122273



select * from work.runfull201903041255_analysis_html_mini
where epi_id = '10006925' and analysis_name_raw like '%Biokeemia analüüs%';


select * from work.run_201908070955analysis_html_cleaned
where epi_id = '10006925' and analysis_name_raw like '%Biokeemia analüüs%';

select work.clean_effective_time('2010-06-17 00:00:00');


select count(*) from work.run_201908081115_analysis_entry;





-----------------------------------------------------------------------------
-- CLEAN ENTRY
-----------------------------------------------------------------------------

set search_path to work;
set role egcut_epi_work_create;

drop table if exists work.run_201908081119_analysis_entry_cleaned;
create table work.run_201908081119_analysis_entry_cleaned as
    with value_type as
        (
        -- determine value type (float, ratio etc)
        select *, check_types(value_raw) as value_type
        from work.run_201908081119_analysis_entry
        limit 10000
        )
    select *,
          clean_analysis_name(analysis_name_raw) as analysis_name,
          clean_parameter_name(parameter_name_raw) as parameter_name,
          clean_effective_time(effective_time_raw) as effective_time,
          clean_value(value_raw, value_type) as value,
          clean_reference_value(reference_values_raw) as reference_values
    from value_type;
--1min asemel 40 sekundit


select value_raw, value, count(*) from work.run_201908081103_analysis_entry_cleaned
where value_type is null
group by value_raw, value
order by count desc;

/*
Neid ei puhasta millegipärast???
"2.10 (2,1)","2.10 (2,1)"
"0.00 (0,0)","0.00 (0,0)"
"2.80 (2,8)","2.80 (2,8)"
*/



create table work.run_201908081117_analysis_entry_cleaned (
    id integer,
    original_analysis_entry_id integer,
    epi_id varchar,
    epi_type varchar,
    analysis_id integer,
    code_system varchar,
    code_system_name varchar,
    analysis_code_raw varchar,
    analysis_name_raw varchar,
    parameter_code_raw varchar,
    parameter_name_raw varchar,
    parameter_unit_raw varchar,
    reference_values_raw varchar,
    effective_time_raw varchar,
    analysis_name varchar,
    value_raw varchar,
    value_type varchar,
    value varchar,
    parameter_name varchar,
    effective_time varchar,
    reference_values varchar);

insert into  work.run_201908081117_analysis_entry_cleaned
with value_type as
        (

        -- determine value type (float, ratio etc)
        select *, check_types(value_raw) as value_type
        from work.run_201908081117_analysis_entry
        limit 10000
        )
    select *,
          clean_analysis_name(analysis_name_raw) as analysis_name,
          clean_parameter_name(parameter_name_raw) as parameter_name,
          clean_effective_time(effective_time_raw) as effective_time,
          clean_value(value_raw, value_type) as value,
          clean_reference_value(reference_values_raw) as reference_values
    from value_type
;

-------------------------------------------------------------------------
-- ALGNE CLEAN_VALUE
drop table if exists  run_201908081119_analysis_entry_cleaned ;
create temp table run_201908081119_analysis_entry_cleaned as
with value_type as
        (
        -- determine value type (float, ratio etc)
        select *, check_types(value_raw) as value_type
        from work.run_201908081119_analysis_entry
        limit 10000
        )
    select *,
          clean_analysis_name(analysis_name_raw) as analysis_name,
          clean_parameter_name(parameter_name_raw) as parameter_name,
          clean_effective_time(effective_time_raw) as effective_time,
          check_types(value_raw) as value_type
          clean_value(value_raw, value_type) as value,
          clean_reference_value(reference_values_raw) as reference_values
    from value_type
limit 10000;--value_type
-- 40sekundit, kui remove units teha 2 korda (limit 10 000)
-- 1 minut algselt algselt (limit 10 000)

-------------------------------------------------------------------------
--UUS CLEAN_VALUE
drop table if exists work.runfull201903041255_analysis_html_cleaned;
create table work.runfull201903041255_analysis_html_cleaned as
    with cleaned_values as
        (
        select *, clean_values(value_raw, parameter_unit_raw) as cleaned_value
        from work.runfull201903041255_analysis_html
        )
    select *,
          clean_analysis_name(analysis_name_raw) as analysis_name,
          clean_parameter_name(parameter_name_raw) as parameter_name,
          clean_effective_time(effective_time_raw) as effective_time,
          cleaned_value[1] as value,
          cleaned_value[2] as parameter_unit,
          clean_reference_value(reference_values_raw) as reference_values
    from cleaned_values;

--kõik fun koos 6.5s sekundit (limit 10 000)
--kõik fun koos 1 min (limit 100 000)


select value, value_raw, parameter_unit_raw, parameter_unit_new, count(*) from run_201908081119_analysis_entry_cleaned
group by value, value_raw, parameter_unit_raw, parameter_unit_new
order by count desc;







drop table if exists  run_201908081119_analysis_entry_cleaned;
--create temp table run_201908081119_analysis_entry_cleaned as
    select count(*) from (
                             select *,
                                    work.check_types(value_raw)
                                    --clean_analysis_name(analysis_name_raw) as analysis_name,
                                    --clean_parameter_name(parameter_name_raw) as parameter_name,
                                    --clean_effective_time(effective_time_raw) as effective_time,
                                    --work.xxx_check_types(value_raw) as value_type
                                    --clean_value(value_raw, value_type) as value,
                                    --clean_reference_value(reference_values_raw) as reference_values
                             from work.run_201908081119_analysis_entry
                             limit 10000--value_type
) as tbl



drop function if exists work.xxx_check_types(text text);
create or replace function work.xxx_check_types(text text)
    returns int
as
$body$
declare
begin
    if work.is_float(text) then return 1; end if;
    if work.is_integer(text) then  return 1; end if;
    if work.is_text(text) then  return 1; end if;
    if work.is_num_and_par(text) then  return 1; end if;
    if work.is_ratio(text) then  return 1; end if;
    if work.is_range(text) then  return 1; end if;
    return '1';
    return NULL;
end
$body$
language plpgsql;

-----------------------------------------------------------------
-- IS FLOAT
-----------------------------------------------------------------

drop function if exists is_float(text text);
create or replace function is_float (text text)
    returns boolean
as
$body$
declare
begin
    if match_float(remove_units(text, True)) --code in common.value has default strict = True
        then
        return True;
        end if;
    return False;
end
$body$
language plpgsql;




drop function if exists work.xxx_remove_units(rawtext text, strict boolean);
create or replace function work.xxx_remove_units (rawtext text, strict boolean)
    returns text
as
$body$
declare
    last_element_index integer;
    last_element text;
    selected_unit text := '';
    pos_unit text;
    text text := trim(both from rawtext); --removing whitespaces
begin
    last_element_index :=  array_length(regexp_split_to_array(text, ' '), 1);
    last_element := split_part(text, ' ', last_element_index); -- last element in text can be unit

    -- unit '%' is special character and needs to be escaped
    text := regexp_replace(text, '%', '\%');

    /*
    -- is unit the last element in text?
    for pos_unit in select * from possible_units loop

        -- unit '%' is special character and needs to be escaped
        if (pos_unit like '%\%%') then pos_unit := regexp_replace(pos_unit, '%', '\%'); end if;

        if (lower(last_element) = lower(pos_unit)     and length(pos_unit) > length(selected_unit) and strict)  or-- last element of text is unit
           (lower(text) like ('%' || lower(pos_unit)) and length(pos_unit) > length(selected_unit) and not strict) -- text endswith unit
            then selected_unit := pos_unit;
        end if;
    end loop;*/

     -- delete unit from the end
     if selected_unit != '' then
         text :=  trim(both from substr(text, 1, length(text) - length(selected_unit)));
     end if;

    -- subsituting the escaped '\%' back to '%'
    --text := regexp_replace(text, '\\%', '%');

    return text;
end
$body$
language plpgsql;

SELECT pid, query FROM pg_stat_activity;
SELECT pg_terminate_backend(23405);

select effective_time_raw from work.runfull201903041255_analysis_html limit 1;

delete from run_201909251053_analysis_html
where row_nr >= 100;

select analysis_substrate_html from run_201909191153_matched;

select id from run_201909251053_analysis_html_loinced_unique;

set search_path to work;

--59 rida
select id, value_type, value_raw, value, parameter_unit_raw, parameter_unit_from_suffix, parameter_unit
from run_201909261018_analysis_html_loinced
where value_type = 'range' and value_raw is not null and value is null and value_raw != '-';

select value_raw_entry, value_raw_html, value_entry, value_html, count(*)
from run_201909261018_matched
    where (value_raw_html is not null and  value_html is null )
       or (value_raw_entry is not null and value_entry is null)
group by  value_raw_entry, value_raw_html, value_entry, value_html
order by count desc;



select distinct value_type, value_raw, value, parameter_unit_from_suffix, parameter_unit
from run_201909261018_analysis_html_loinced
where value_raw is not null and value is null and value_raw != '-';

select xxx_is_range('<0,10 ');



set role egcut_epi_work_create;
drop function if exists xxx_is_range(text text);
create or replace function xxx_is_range (text text)
    returns boolean
as
$body$
declare
    int_pattern varchar :=  '(0|[1-9][0-9]*)';
    float_pattern varchar := '(([0-9]|[1-9][0-9]*)?\s*[\.,][0-9]*)';
    number_pattern varchar := format('([+-]?(%s|%s))', float_pattern, int_pattern);

    range_pattern1 varchar := concat('(', '\s*[>=<=><]{1,2}\s*', number_pattern, '\s*$)'); -- <3 <=3 >3 >=3
    range_pattern2 varchar := concat('(', number_pattern, '\s*\-\s*', number_pattern, '$)'); -- 3-4 PROBLEEM 4-4-5
    range_pattern3 varchar := concat('(', number_pattern, '\s*\.\.\s*', number_pattern, ')'); -- 3..4
    range_pattern varchar := format('(^%s|^%s|^%s)', range_pattern1, range_pattern2, range_pattern3);
begin
    if text ~ range_pattern then return TRUE; end if;
    return FALSE;
end
$body$
language plpgsql;




--ALGNE
create function is_range(text text) returns boolean
    language plpgsql
as
$$
declare
    int_pattern varchar :=  '(0|[1-9][0-9]*)';
    float_pattern varchar := '(([0-9]|[1-9][0-9]*)?\s*[\.,][0-9]*)';
    number_pattern varchar := format('([+-]?(%s|%s))', float_pattern, int_pattern);

    range_pattern1 varchar := concat('(', '\s*[>=<=><]{1,2}\s*', number_pattern, ')'); -- <3 <=3 >3 >=3
    range_pattern2 varchar := concat('(', number_pattern, '\s*\-\s*', number_pattern, '$)'); -- 3-4 PROBLEEM 4-4-5
    range_pattern3 varchar := concat('(', number_pattern, '\s*\.\.\s*', number_pattern, ')'); -- 3..4
    range_pattern varchar := format('(^%s|^%s|^%s)', range_pattern1, range_pattern2, range_pattern3);
begin
    if text ~ range_pattern then return TRUE; end if;
    return FALSE;
end
$$;



