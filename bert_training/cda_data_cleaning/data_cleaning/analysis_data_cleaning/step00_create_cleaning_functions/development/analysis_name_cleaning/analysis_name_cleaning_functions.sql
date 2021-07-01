drop function if exists work.clean_analysis_name(analysis_name_raw text);
set role egcut_epi_work_create;
create or replace function work.clean_analysis_name(analysis_name_raw text)
    returns text
    as
$body$
declare
    analysis_name varchar := analysis_name_raw;
begin

    if analysis_name_raw is not null then
        --removes repeating white spaces[11,16]
        analysis_name := trim(regexp_replace(analysis_name_raw, '\s+', ' ', 'g'));

        --a1 ALAT  ->	ALAT
        --a20 ASAT ->	ASAT
        --a139 Valk->	Valk
        --a2041 Hb ->	HB
        -- TODO SQL JAOKS TOPELT { }
        -- a2042 etc are parameter_codes
        if(analysis_name_raw ~ '^[a][0-9]{4}') then analysis_name := regexp_replace(analysis_name, '^[a][0-9]{4}',''); end if;
        if(analysis_name_raw ~ '^[a][0-9]{3}') then analysis_name := regexp_replace(analysis_name, '^[a][0-9]{3}',''); end if;
        if(analysis_name_raw ~ '^[a][0-9]{2}') then analysis_name := regexp_replace(analysis_name, '^[a][0-9]{2}',''); end if;
        if(analysis_name_raw ~ '^[a][0-9]{1}') then analysis_name := regexp_replace(analysis_name, '^[a][0-9]{1}',''); end if;

        -- unify ANONYM
        -- <ANONYM id="10" type="per" morph="_S_ pl g"/> fraktsioonid seerumis elektroforeetiliselt -> <ANONYM> fraktsioonid seerumis elektroforeetiliselt
        if(analysis_name_raw like '%ANONYM%') then analysis_name := regexp_replace(analysis_name, '<ANONYM(.+)/>', '<ANONYM>'); end if;

        -- removes whitespaces from begin and end
        analysis_name := trim(both from analysis_name);
    end if;

    if analysis_name_raw is null or analysis_name_raw = 'Parameetri nimetus' or analysis_name_raw = 'Paramteeri nimetus' then
        analysis_name := null;
    end if;

    return analysis_name;
end
$body$
language plpgsql;

select * from work.run_ac_html_201901021226_analysis_html limit 1;

select regexp_replace('<ANONYM id="10" type="per" morph="_S_ pl g"/> fraktsioonid seerumis elektroforeetiliselt', '<ANONYM(.+)/>', '<ANONYM>')

select distinct analysis_name_raw, work.clean_analysis_name(analysis_name_raw)
from work.run_201907231010_analysis_html_loinced;
--where  work.clean_analysis_name(analysis_name_raw) != analysis_name_raw;

select 'a2033 Hemogramm' ~ '^[a][0-9]{4}';  --regexp_replace('a2033 Hemogramm', '^[a][0-9]{4}','');

select distinct work.clean_analysis_name(analysis_name_raw), analysis_name_raw
from work.runfull201902260943_analysis_html
where analysis_name_raw !=  work.clean_analysis_name(analysis_name_raw);


select *, work.parameter_name_clean(parameter_name_raw) as parameter_name,
                                            work.clean_value(value_raw, value_type) as value from
                                            (
                                                    -- determine value type (float, ratio etc)
                                                    select *, work.check_types(value_raw) as value_type
                                                    from work.run_ac_html_201903081142_analysis_html_mini
                                            ) as tbl;

set search_path to work;
with value_type as
    (
    -- determine value type (float, ratio etc)
    select *, check_types(value_raw) as value_type
    from work.run_ac_html_201903081142_analysis_html_mini
    )
select *,
       clean_parameter_name(parameter_name_raw) as parameter_name,
       clean_value(value_raw, value_type) as value,
       clean_analysis_name(analysis_name_raw) as analysis_name
from value_type;

select * from work.run_ac_html_201903081142_analysis_html_mini limit 1;


set search_path to work;
select work.is_float('3.0'); --;from work.run_ac_html_201902130935_analysis_html;

select work.match_ratio('3/4');


-----------------------------------------------------------------------
-- asukohas cda-data-cleaning/ analysis_data_cleaning / name_cleaning/  cleaning.py on veel võimalikke puhastamise viise

select * from work.runfull201903041255_analysis_html
where parameter_name_raw like '%abs%' or analysis_name_raw like '%abs%';