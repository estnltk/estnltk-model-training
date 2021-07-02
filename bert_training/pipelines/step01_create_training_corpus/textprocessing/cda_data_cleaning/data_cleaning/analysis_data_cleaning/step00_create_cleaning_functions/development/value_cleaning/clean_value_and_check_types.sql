-- funktsioon split_value_and_unit
-- kaustas /home/anneott/Repos/cda-data-cleaning/cda_data_cleaning/data_cleaning/step02_clean_analysis_html/development/split_value_and_unit.sql

-- EESMÄRK
   -- 1. võtab sisse value_raw, splitib (prefix = value, suffix = unit)-ks  			                        sisemine select
   -- 2. kui unit in possible units, siis määrab type value järgi (sest value_raw veergu sattunud ka unit)      keskmine select
        --  kui mitte, siis määrab type value_raw järgi
   -- 3. nüüd puhastab clean(mingi value v value_raw, type), SIIN ENAM SPLITMIST/REMOVIMIST POLE TARVIS         välimine select


-- Determines value_type of a value
-- Based on value_type cleans the table
-- Checks if unit has been accidentally placed under value column, CHOOSES THE RIGHT UNIT

-- new_value = value without unit
-- new_parameter_unit = unit gotten from value column



-- võtab sisse value_raw ja parameter_unit_raw, tagastab value (puhastatud) ja parameter_unit (potentsiaalselt uus unit)
-- 1. Assigning value_type
	-- if suffix is actually a unit then value := prefix, parameter_unit := suffix
	-- if suffix is not a unit everything is left the same
-- 2. Placing units from 'value_raw' column to 'parameter_unit' column
	-- if unit had accidentally been placed under 'value_raw' (and there is no unit under 'parameter_unit_raw') then move it under 'parameter_unit'
-- 3. Clean all the values based on their type
set search_path to work;
drop function if exists xxx_clean_values(value_raw text, parameter_unit_raw varchar); --, OUT new_value varchar, OUT new_parameter_unit varchar);
create or replace function xxx_clean_values(value_raw text, parameter_unit_raw varchar) --, new_value varchar, new_parameter_unit varchar)
    returns text[]
as
$body$
declare
    value_type varchar;
    new_value varchar;
    new_parameter_unit varchar;
    parameter_unit varchar;

    prefix varchar := trim((split_value_and_unit(value_raw)).prefix);
    suffix varchar := trim((split_value_and_unit(value_raw)).suffix);
begin
    if value_raw is null then return array[value_raw, parameter_unit_raw, suffix, NULL]; end if;

    -- 1. FINDING VALUE_TYPE
    -- suffix is actually a unit that has accidentally placed under 'value' column
    if suffix in (select unit from possible_units) then
        value_type := check_types(prefix);
        new_parameter_unit := suffix;
        new_value := prefix;
    -- suffix is not a unit
    else value_type := check_types(value_raw);
         new_value := value_raw;
    end if;


    -- 2. CHECK IF UNIT HAS BEEN ACCIDENTALLY PLACED UNDER VALUE COLUMN
    if new_parameter_unit is not null and parameter_unit_raw is null THEN parameter_unit := new_parameter_unit;
    else parameter_unit := parameter_unit_raw;
    end if;


    -- 3. CLEANING VALUE
    --removes repeating white spaces
    new_value := trim(regexp_replace(new_value, '\s+', ' ', 'g'));

    if value_type = 'float'       then return array[clean_floats(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'integer'     then return array[clean_integer(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'text'        then return array[new_value, parameter_unit, suffix, value_type]; end if; --don't know how to clean text
    if value_type = 'num_and_par' then return array[match_num_and_par(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'ratio'       then return array[match_ratio(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'range'       then return array[match_range(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'time_series' then return array[new_value, parameter_unit, suffix, value_type]; end if; --no cleaning yet!!!
    if value_type = 'text_and_value' then return array[new_value, parameter_unit, suffix, value_type]; end if; --no cleaning yet!!!

    return array[new_value, parameter_unit, suffix, value_type];

    end
$body$
language plpgsql;

--
select * from possible_units;

-- ESIMENE LIIGE ON PUHAS VALUE, TEINE ON UNIT

select clean_values('<= 0.2', NULL); --  korras
select clean_values('<= 0.2', 'fL'); --  korras
select xxx_clean_values('1.020 (1,020)', 'fL'); --   korras

-- PU == NULL
select xxx_clean_values('2..4 fL', NULL); --korras
select xxx_clean_values('4.0-5.9 fL', NULL); --korras
select xxx_clean_values('1.020 (1,020) fL', NULL); --  korras
select xxx_clean_values('<9 fL', NULL); --  korras
select xxx_clean_values('3:4 fL', NULL); -- korras
select xxx_clean_values('3-4 fL', NULL); -- korras
select xxx_clean_values('<4 fL', NULL); -- korras
select xxx_clean_values('4 fL', NULL); --korras
select xxx_clean_values('4.0 fL', NULL); --korras
select xxx_clean_values('4(4) fL',  NULL); --korras
select xxx_clean_values('4.0(4.0) fL',  NULL); --korras


----------------------------------------------------------------------------------------------------------------

drop function if exists xxx_clean_values(value_raw text, parameter_unit_raw varchar); --, OUT new_value varchar, OUT new_parameter_unit varchar);
create or replace function xxx_clean_values(value_raw text, parameter_unit_raw varchar) --, new_value varchar, new_parameter_unit varchar)
    returns text[]
as
$body$
declare
    value_type varchar;
    new_value varchar;
    new_parameter_unit varchar;
    parameter_unit varchar;

    prefix varchar := trim((split_value_and_unit(value_raw)).prefix);
    suffix varchar := trim((split_value_and_unit(value_raw)).suffix);
begin
    if value_raw is null then return array[value_raw, parameter_unit_raw]; end if;

    -- 1. FINDING VALUE_TYPE
    -- suffix is actually a unit that has accidentally placed under 'value' column
    if suffix in (select unit from possible_units) then
        value_type := xxx_check_types(prefix);
        new_parameter_unit := suffix;
        new_value := prefix;
    -- suffix is not a unit
    else value_type := xxx_check_types(value_raw);
         new_value := value_raw;
    end if;

    -- 2. CHECK IF UNIT HAS BEEN ACCIDENTALLY PLACED UNDER VALUE COLUMN
    if new_parameter_unit is not null and parameter_unit_raw is null THEN parameter_unit := new_parameter_unit;
    else parameter_unit := parameter_unit_raw;
    end if;


    -- 3. CLEANING VALUE
    --removes repeating white spaces
    new_value := trim(regexp_replace(new_value, '\s+', ' ', 'g'));

    if value_type = 'float'       then return array[clean_floats(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'integer'     then return array[clean_integer(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'text'        then return array[new_value, parameter_unit, suffix, value_type]; end if; --don't know how to clean text
    if value_type = 'num_and_par' then return array[match_num_and_par(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'ratio'       then return array[match_ratio(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'range'       then return array[match_range(new_value), parameter_unit, suffix, value_type]; end if;
    if value_type = 'time_series' then return array[new_value, parameter_unit, suffix, value_type]; end if; --no cleaning yet

    return array[new_value, parameter_unit,suffix, value_type];
    end
$body$
language plpgsql;

select suffix, count(*), array_agg(distinct value_raw)
from (
               select clean_value[1] as value,
                      value_raw,
                      clean_value[2] as parameter_unit,
                      clean_value[3] as suffix,
                      clean_value[4] as value_type
                from (
                        select value_raw,
                               xxx_clean_values(value_raw, parameter_unit_raw) as clean_value
                        from runfull201902180910_analysis_html
                        --where value_raw ~
                        --      '^((([0-9]|[1-9][0-9]*)?[\.,][0-9]*)\((0|[0-9][0-9]*):(0|[0-9][0-9]*)\),)*$'
                    limit 80000
                    ) as tbl
               ) as tbl2
group by suffix
order by count desc;

6-8



select xxx_clean_values('0.2(19:45),0.3(22:46),', '');








--------------------------------------------------------------------------------------------------------------------
--CREATING A NEW CLEANED TABLE WITH PROPER UNITS
--------------------------------------------------------------------------------------------------------------------
select (xxx_clean_values(value_raw, parameter_unit_raw))[1] as value,
        (xxx_clean_values(value_raw, parameter_unit_raw))[2] as parameter_unit,
       *
    from run_201908081119_analysis_entry;


-- populaarseimad kombonatsioonid
select * ,count(*) from
    (select (xxx_clean_values(value_raw, parameter_unit_raw))[1] as value,
           value_raw,
           (xxx_clean_values(value_raw, parameter_unit_raw))[2] as parameter_unit,
           parameter_unit_raw
    from run_201908081119_analysis_entry) as tbl
group by value, value_raw, parameter_unit, parameter_unit_raw
order by count desc;

----------------------------------------------------------------------------------------------------------------------

 /*select * from (
                   select (xxx_clean_values(value_raw, parameter_unit_raw))[1] as value,
                          (xxx_clean_values(value_raw, parameter_unit_raw))[2] as parameter_unit,
                          *
                   from run_201908081119_analysis_entry
               ) as tbl
where parameter_unit is distinct from parameter_unit_raw;
*/
--epi_id = '53573192' and analysis_name_raw = 'PT-INR'

/*
with value_type_table as
    (
    select (xxx_check_type(value_raw)).new_value,
           (xxx_check_type(value_raw))./µLnew_parameter_unit,
           (xxx_check_type(value_raw)).value_type,
           *
    from run_201908081119_analysis_entry
    )
select xxx_clean_value(new_value, value_type) as value,
       -- PARAMETER_UNIT
       -- placing parameter units that were under 'value_raw' column to 'parameter_unit' column
       CASE WHEN new_parameter_unit is NOT NULL and parameter_unit_raw IS NULL THEN new_parameter_unit
            ELSE parameter_unit_raw
       END AS parameter_unit,
       *
from value_type_table;
*/
------------------------------------------------------------------------------
--CHECK TYPE
------------------------------------------------------------------------------
drop function if exists xxx_check_types(text text);
create or replace function xxx_check_types(text text)
    returns text
as
$body$
declare
begin

    if is_float(text) then return 'float'; end if;
    if is_integer(text) then return 'integer'; end if;
    if is_text(text) then return 'text'; end if;
    if is_num_and_par(text) then return 'num_and_par'; end if;
    if xxx_is_ratio(text) then return 'ratio'; end if;
    if xxx_is_range(text) then return 'range'; end if;
    if is_time_series(text) then return 'time_series'; end if;
    if is_text_and_value(text) then return 'text_and_value'; end if;
    return NULL;
end
$body$
language plpgsql;


/*
------------------------------------------------------------------------------
--CLEAN_VALUE
------------------------------------------------------------------------------

drop function if exists xxx_clean_value(value_raw text, type text);
create or replace function xxx_clean_value(value_raw text, type text)
    returns text
as
$body$
declare
begin

    --removes repeating white spaces
    value_raw := trim(regexp_replace(value_raw, '\s+', ' ', 'g'));

    if type = 'float' then return clean_floats(value_raw); end if;
    if type = 'integer' then return clean_integer(value_raw); end if;
    if type = 'text' then return value_raw; end if; --don't know how to clean text
    if type = 'num_and_par' then return match_num_and_par(value_raw); end if;
    if type = 'ratio' then return match_ratio(value_raw); end if;
    if type = 'range' then return match_range(value_raw); end if;
    return value_raw;

end
$body$
language plpgsql;

*/
------------------------------------------------------------------------------
--CLEANING
------------------------------------------------------------------------------

with value_type_table as
    (
    select (xxx_check_type(value_raw)).new_value,
           (xxx_check_type(value_raw)).new_parameter_unit,
           (xxx_check_type(value_raw)).value_type,
           *
    from run_201908081119_analysis_entry
    )
select xxx_clean_value(new_value, value_type) as value,
       -- PARAMETER_UNIT
       -- placing parameter units that were under 'value_raw' column to 'parameter_unit' column
       CASE WHEN new_parameter_unit is NOT NULL and parameter_unit_raw IS NULL THEN new_parameter_unit
            ELSE parameter_unit_raw
       END AS parameter_unit,
       *
from value_type_table;



--where epi_id = '53573192' and analysis_name_raw = 'PT-INR';
-- uus unit 53573192, an PT-INR
-- tavaline 35989970

--kui new_parameter_unit == NULL -> parameter_unit = parameter_unit_raw
--kui new_parameter_unit != NULL -> parameter_unit = new_parameter_unit






--------------------------------------------------------------------------------------------------------------------------
select *, clean_value() FROM (
                -- assigning value type
                -- siin peaks ka uniti alles jätma
                  select *,
                        (CASE WHEN value_and_unit.suffix     in (select unit from work.possible_units) then check_types(prefix)
                              WHEN value_and_unit.suffix not in (select unit from work.possible_units) then check_types(value_raw) end)
                              as value_type
                  from (
                        -- uniti ja value eraldamine
                           select value_raw,
                                  trim((split_value_and_unit(value_raw)).value) as prefix,
                                  trim((split_value_and_unit(value_raw)).unit) as suffix
                           from run_201908081119_analysis_entry
                       ) as value_and_unit
              ) as tbl;


select check_types('POS tiitris 1:100');

-- EI TÖÖTA
-- ei määra tüüpi