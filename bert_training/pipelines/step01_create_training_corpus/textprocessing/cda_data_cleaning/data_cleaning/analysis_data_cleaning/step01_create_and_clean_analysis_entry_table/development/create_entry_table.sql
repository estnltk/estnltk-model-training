set role egcut_epi_work_create;
create table work.run_201907301617_analysis_entry_raw as
select effective_time_raw, parameter_name_raw, analysis_name_raw, value_raw,
                parameter_unit_raw, reference_values_raw
from work.runfull201902071713_analysis_entry
limit 1000;

set search_path to work;
select *,
       clean_analysis_name(analysis_name_raw) as analysis_name,
       clean_parameter_name(parameter_name_raw) as parameter_name,
       clean_value(value_raw) as value
from work.run_201907301617_analysis_entry_raw

select * from work.run_201907301617_analysis_entry_raw limit 10;

drop table if exists work.runfull201903041255_analysis_html_less_col;

set role egcut_epi_work_create;
create table work.runfull201903041255_analysis_html_less_col as
select epi_id,  analysis_name_raw, parameter_name_raw, value_raw, reference_values_raw, effective_time_raw, analysis_substrate_raw
from work.runfull201903041255_analysis_html_new
limit 5000;