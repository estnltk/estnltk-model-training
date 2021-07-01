set search_path to work;

set role egcut_epi_work_create;
drop function if exists is_time_series(value_raw  varchar);--, OUT all_patterns varchar);
create or replace function is_time_series(value_raw varchar)
    returns boolean
as
$body$
declare
    int_pattern varchar :=  '(0|[1-9][0-9]*)';
    time_int_pattern varchar := '((0[0-9]|1[0-9]|2[0-3]|[0-9]):[0-5][0-9])';
    float_pattern varchar := '(([0-9]|[1-9][0-9]*)?[\.,][0-9]*)';
    number_pattern varchar := format('([+-]?(%s|%s))', float_pattern, int_pattern);

    range_pattern1 varchar := concat('(', '\s*[>=<=><]{1,2}\s*', number_pattern, ')');
    range_pattern2 varchar := concat('(', number_pattern, '\s*\-\s*', number_pattern, ')'); -- 3-4 PROBLEEM 4..3 ka true
    range_pattern3 varchar := concat('(', number_pattern, '\s*\.\.\s*', number_pattern, ')');
    range_pattern varchar := format('(%s|%s|%s)', range_pattern1, range_pattern2, range_pattern3);

    value_pattern varchar := concat('(', range_pattern, '|',  number_pattern, ')');

    time_series_pattern varchar := concat('^(', value_pattern, '\(', time_int_pattern, '\),', ')+$');
begin
    if value_raw ~ time_series_pattern then return True; end if;
    return False;
end
$body$
language plpgsql;

select is_time_series('<0,010(00:00),<0,010(09:13),');
select * from is_time_series('51.3(05:15),42.4(11:35),40.1(17:19),39.6(22:01),'); -- ei toimi
select * from is_time_series('6,8(02:39),26,5(02:39),6,9(02:39),22,9(02:39),'); --toimib
select * from is_time_series('48.3(05:15),42.4(17:23),');
select * from is_time_series('6,37(05:15),4,92(11:35),5,04(17:19),3,90(22:01),');
select * from is_time_series('97(19:45),103(22:46),');

-- ei tööta õigesti
select * from is_time_series('<3'); ---- true ??
select * from is_time_series('3..4'); --true ??
select * from is_time_series('1-0'); -- true ??
select * from is_time_series('0-1-0-1'); -- true ??
select * from is_time_series('4:3'); --false



-- vaja puhastada sellsied floatid: 10,7(06:46),4,4(06:46),
drop function if exists clean_time_series(value_raw  varchar);
create or replace function clean_time_series(value_raw varchar)
    returns boolean
as
$body$
declare

begin

end
$body$
language plpgsql;

drop function if exists xxx_split_value_and_unit(value_raw text, OUT prefix varchar, OUT suffix varchar);
create or replace function xxx_split_value_and_unit(value_raw text, OUT prefix varchar, OUT suffix varchar)
as
$body$
declare
    int_pattern varchar :=  '(0|[1-9][0-9]*)';
    float_pattern varchar := '(([0-9]|[1-9][0-9]*)?\s*[\.,][0-9]*)';
    number_pattern varchar := format('([+-]?(%s|%s))', float_pattern, int_pattern);

    ratio_pattern varchar := format('(%s\s*[\:/]\s*%s)', number_pattern, number_pattern); -- 3:4 või 3/4

    range_pattern1 varchar := concat('(', '\s*[>=<=><]{1,2}\s*', number_pattern, ')'); -- <3 <=3 >3 >=3
    range_pattern2 varchar := concat('(', number_pattern, '\s*\-\s*', number_pattern, ')'); -- 3-4 PROBLEEM 4..3 ka true
    range_pattern3 varchar := concat('(', number_pattern, '\s*\.\.\s*', number_pattern, ')'); -- 3..4
    range_pattern varchar := format('(%s|%s|%s)', range_pattern1, range_pattern2, range_pattern3);

    num_and_par_pattern varchar := format('(^%s\s*\(%s\))', number_pattern, number_pattern); --3(3) PROBLEEM 5(3) ka TRUE, kuigi peaks olema FALSE

    value_pattern varchar := concat('(^\s*(', number_pattern, ')|(', ratio_pattern,')|(', range_pattern, ')|(', num_and_par_pattern, '))');
    removal_pattern varchar := concat(value_pattern, '\s*');
begin
    value_raw := trim(value_raw);
    prefix := substring(value_raw, value_pattern);
    suffix := regexp_replace(value_raw, removal_pattern, '');
end
$body$
language plpgsql;

select work.xxx_split_value_and_unit('<9 g');
select work.xxx_split_value_and_unit('5:7 g');
-- 5..9 toimib
-- <3 ei toimi
-- 5-9 toimib
-- 5:7 toimib
-- 5(5) toimib
-- 5.9 toimib









select split_part(value, '),', 1), split_part(value, '),', 2), split_part(value, '),', 3), value from run_201909021100_analysis_html_loinced
where value_type = 'time_series';

select * from (
        select  split_part(value, ',', 1), split_part(value, ',', 2) as s2, split_part(value, ',', 3), value
        from run_201909021100_analysis_html_loinced
        where value_type = 'text_and_value') as tbl
where s2 != '';

with table1 as (
    select value, is_time_series(value) as is_time from run_201909021100_analysis_html_loinced)
select value, count(*)
from table1
where is_time is True
group by value
order by count desc;


select clean_floats('0.35 %');
select clean_integer('0.35 %');
select match_between('0.35 %');

select * from run_201909111308_analysis_entry where value_raw = '0.35 %';




-- repeating
/*            ▪ Entry | html
            ▪ B | 	"B(13:28),B(13:28),"
            ▪ POS |   "POS(13:28),POS(13:28),"
            ▪ SOBIB |  SOBIB(13:28),SOBIB(13:28),"

 */
select 'SOBIB(13:28),SOBIB(13:28),' ~ '/(\b\S+\b)\s+\b\1\b/';

select 'SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),' ~ '([\w+\d+])\1';
select 'SOBIB(13:28),SOBIB(13:28),' ~ '/(.)(?=.*\1)/g';


select 'SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),' ~ '(\w){2,}';

select regexp_replace('SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),','(.)\1{1,}', '\1', 'g');
select regexp_replace('SOBIB(13:28),SOBIB(13:28),', '\b(.+,)\1\b', '');

select regexp_replace('SOBIB(13:28),SOBIB(13:28),','(^| )([^ ]+ )(.*? )?\2+','\1\2\3');

select regexp_replace('sobib,sobib,','(^| )([^ ]+ )(.*? )?\2+','\1\2\3');

select regexp_replace('sobib,sobib,','(![^!]+!).*\1', '');

--!!!! töötab aga ainult rohkem kui 3 korduse puhul
select REGEXP_REPLACE('SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),', '((.){2,})\1', '');

--ei tööta, sest ainult 2 kordust
select REGEXP_REPLACE('SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),', '((.){2,})\1', '');

--ei tööta paaris arvudega, töötab paaritutega
select REGEXP_REPLACE('SOBIB(13:28),SOBIB(13:28),', '(.+)\1', '');
select REGEXP_REPLACE('SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),', '(.+)\1', '');

select REGEXP_REPLACE('SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),', '(.*)', '\1');

select REGEXP_REPLACE('SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),', '.(.)\1', '');

select REGEXP_REPLACE('sobib,sobib,sobib,sobib,', '\b(\w+)\1\b', '\1');


select REGEXP_REPLACE('SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),SOBIB(13:28),', '(.{1,})', '\1');


select 'sobib,sobib,sobib,' ~ '(.){1,}';

select 'B(13:28),B(13:28),' ~ '((.){2,})\1';






SET search_path TO work;
set role egcut_epi_work_create;
create table run_201909111308_analysis_html_cleaned as
    with cleaned_values as
        (
        select *, clean_values(value_raw, parameter_unit_raw) as cleaned_value
        from runfull201903041255_analysis_html
        )
    select *,
          clean_analysis_name(analysis_name_raw) as analysis_name,
          clean_parameter_name(parameter_name_raw) as parameter_name,
          clean_effective_time(effective_time_raw) as effective_time,
          cleaned_value[1] as value,
          cleaned_value[2] as parameter_unit_from_suffix,
          cleaned_value[3] as suffix,
          cleaned_value[4] as value_type,
          clean_reference_value(reference_values_raw) as reference_values
    from cleaned_values;