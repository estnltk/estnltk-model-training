-- vaja seda tüüpi ära tunda ja kaheks splittida
--Negatiivne (<12), "Negatiivne (1,99)"

set role egcut_epi_work_create;
drop function if exists is_text_and_value(value_raw  varchar);
reset role;
create or replace function is_text_and_value(value_raw varchar)
    returns boolean
as
$body$
declare
    int_pattern varchar :=  '(0|[1-9][0-9]*)';
    time_int_pattern varchar :=  '(0|[0-9][0-9]*)';
    float_pattern varchar := '(([0-9]|[1-9][0-9]*)?[\.,][0-9]*)';
    number_pattern varchar := format('([+-]?(%s|%s))', float_pattern, int_pattern);

    ratio_pattern varchar := format('(%s\s*[\:/]\s*%s)', number_pattern, number_pattern); -- 3:4 või 3/4

    range_pattern1 varchar := concat('(', '\s*[>=<=><]{1,2}\s*', number_pattern, ')'); -- <3 <=3 >3 >=3
    range_pattern2 varchar := concat('(', number_pattern, '\s*\-\s*', number_pattern, ')'); -- 3-4 PROBLEEM 4..3 ka true
    range_pattern3 varchar := concat('(', number_pattern, '\s*\.\.\s*', number_pattern, ')'); -- 3..4
    range_pattern varchar := format('(%s|%s|%s)', range_pattern1, range_pattern2, range_pattern3);

    num_and_par_pattern varchar := format('(%s\s*\(%s\))', number_pattern, number_pattern); --3(3) PROBLEEM 5(3) ka TRUE, kuigi peaks olema FALSE

    time_series_pattern varchar := concat('^(',number_pattern, '\(', time_int_pattern, ':', time_int_pattern, '\),', ')*$');

    value_pattern varchar := concat('(\s*(', number_pattern, ')|(', ratio_pattern,')|(', range_pattern, ')|(', num_and_par_pattern, ')|(', time_series_pattern, '))');
    text_and_value_pattern varchar := concat('^\w+\(', value_pattern, '\)');
begin
    if value_raw ~ text_and_value_pattern then return True; end if;
    return False;
end
$body$
language plpgsql;

--true
select is_text_and_value('Neg(4)');
select is_text_and_value('Neg(4.3)');
select is_text_and_value('Neg(4:5)');
select is_text_and_value('Neg(4-5)');
select is_text_and_value('Neg(<4)');
select is_text_and_value('Neg(<=4)');
select is_text_and_value('Neg(4(4))'); --false millegipärast????


--false
select is_text_and_value('Neg(neg)');
select is_text_and_value('(4)');
select is_text_and_value('(4.3)');
select is_text_and_value('(4:5)');
select is_text_and_value('(4-5)');
select is_text_and_value('(<4)');
select is_text_and_value('(<=4)');
select is_text_and_value('(4(4))');










select parameter_name_raw, count(*) from work.run_201908081115_analysis_entry_cleaned
where code_system_name = 'LOINC'
group by parameter_name_raw;



select parameter_name_raw, parameter_code_raw, count(*) from work.run_201908081115_analysis_entry_cleaned
where code_system_name = 'LOINC'
group by parameter_name_raw, parameter_code_raw;



select code_system_name, count(*) from work.run_201908081115_analysis_entry_cleaned
where parameter_name_raw is null
group by code_system_name


select * from work.run_201908081115_analysis_entry_cleaned
where lower(parameter_code_raw) like '%par%';

--code_system_name = 'LOINC' on kokku 34658 rida
-- 635 on olemas parameter_name_raw
select count(*) from work.run_201908081115_analysis_entry_cleaned
where code_system_name = 'LOINC' --and parameter_name_raw is not null
--group by parameter_name_raw, parameter_name;


