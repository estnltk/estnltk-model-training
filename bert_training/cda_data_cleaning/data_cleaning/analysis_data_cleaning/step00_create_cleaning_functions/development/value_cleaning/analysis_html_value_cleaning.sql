select value_raw, count(*) from work.runfull201903041255_analysis_html
group by value_raw
order by count desc;

-- 1. check if value_raw and value columns exist
-- 2. check
set role egcut_epi_analysiscleaning_create;
set role egcut_epi_work_create
create table work.aaatest1 as select * from work.runfull201903041255_analysis_html limit 50;
reset role;


--check column existence
drop function if exists work.exist_columns(source_schema text, source_table text);
create or replace function {schema}.exist_columns(source_schema text, source_table text)
                         returns boolean
                            as
                    $body$
                    declare
                        columns text[] := (select array_agg(column_name) from information_schema.columns
                            where table_schema = source_schema and table_name = source_table);
                    begin
                        --required columns
                        if ('value_raw') = any(columns)
                        then return TRUE;
                        end if;
                        return FALSE;
                    end
                    $body$
                    language plpgsql;

select work.exist_columns('work', 'aaatest1');


ALTER TABLE analysiscleaning.aaatest1
DROP COLUMN value;

create table {schema}.{target_table} as
-- clean according to value type
select *, analysiscleaning.clean_value(value_raw, value_type) as value from (
     -- determine value type
    select *, analysiscleaning.check_types(value_raw) as value_type from analysiscleaning.aaatest1
    ) as tbl;



--check
select count(*)from (
-- clean according to value type
                  select *, analysiscleaning.clean_value(value_raw, type) as value
                  from (
                           -- determine value type
                           select value_raw, analysiscleaning.check_types(value_raw) as type
                           from work.runfull201903041255_analysis_html) as tbl1
              ) as tbl2
where value_raw != value and type != 'float';


--work schemasse table possible units
drop table if exists work.possible_units;
set role egcut_epi_analysiscleaning_create;
create table work.possible_units as select * from analysiscleaning.possible_units;
reset role;

--- miks apply_loinc_mapping ei tööta?
drop table if exists  work.run_ac_html_201903081142_analysis_html_mini;
set role egcut_epi_work_create;
create table work.run_ac_html_201903081142_analysis_html_mini as
    select * from work.run_ac_html_201903081142_analysis_html limit 5000;
reset role;

select * FROM work.run_ac_html_201903081142_analysis_html_mini;

SET ROLE egcut_epi_work_create;
create or replace function work.parameter_name_clean(parameter_name text)
                    returns text
                    as
                $body$
                declare
                    parameter_name_clean text := parameter_name;
                begin

                    --a1 ALAT  ->	ALAT
                    --a20 ASAT ->	ASAT
                    --a139 Valk->	Valk
                    --a2041 Hb ->	HB
                    if(parameter_name_clean ~ '^[a][0-9]{{4}}') then parameter_name_clean := regexp_replace(parameter_name_clean, '^[a][0-9]{{4}}','');end if;
                    if(parameter_name_clean ~ '^[a][0-9]{{3}}') then parameter_name_clean := regexp_replace(parameter_name_clean, '^[a][0-9]{{3}}','');end if;
                    if(parameter_name_clean ~ '^[a][0-9]{{2}}') then parameter_name_clean := regexp_replace(parameter_name_clean, '^[a][0-9]{{2}}','');end if;
                    if(parameter_name_clean ~ '^[a][0-9]{{1}}') then parameter_name_clean := regexp_replace(parameter_name_clean, '^[a][0-9]{{1}}','');end if;


                    --Fibrinogeen plasmas* -> Fibrinogeen plasmas
                    if(parameter_name_clean like '%*') then  parameter_name_clean = substr(parameter_name_clean, 1, length(parameter_name_clean) - 1);end if;

                    -- Removing parentheses
                    -- Works only for single pair of parentheses
                    -- WBC(valgeverelible) -> WBC
                    if(parameter_name_clean ~ '(.*)\((.*)\)') then  parameter_name_clean = regexp_replace(parameter_name_clean, '\(.*?\)','');end if;

                    -- % and # from beginning to end
                    -- %NEUT -> NEUT%
                    -- #NEUT -> NEUT#
                    if(parameter_name_clean LIKE '\%%') then parameter_name_clean = substr(parameter_name_clean, 2, 8000) || '%';end if;
                    if(parameter_name_clean LIKE '\#%') then parameter_name_clean = substr(parameter_name_clean, 2, 8000) || '#';end if;

                    --replacing multiple whitespaces with one
                    parameter_name_clean = trim(regexp_replace(parameter_name_clean, '\s+', ' ', 'g'));
                    return parameter_name_clean;
                end
                $body$
                language plpgsql;
reset role;

alter table work.runfull201903041255_analysis_html_mini
                        add column loinc_code varchar,
                        add column elabor_t_lyhend varchar;
                        --add column parameter_name_clean varchar;

--update work.runfull201903041255_analysis_html_mini
--set parameter_name_clean = work.parameter_name_clean(parameter_name_raw);

create table  work.runfull201903041255_analysis_html_mini_loinced as
select *, work.parameter_name_clean(parameter_name_raw) from work.runfull201903041255_analysis_html_mini;



-- olemas PN clean, kuidas saada value_clean juurde?

SET search_path TO work;
--create table {target_schema}.{target_table} as
                                -- clean according to value type

                                select *,
                                       work.clean_value(value_raw, value_type) as value,
                                       work.parameter_name_clean(parameter_name_raw) as parameter_name from
                                        (
                                        -- determine value type (float, ratio etc)
                                        select *, work.check_types(value_raw) as value_type
                                        --!!!! from work.runfull201903041255_analysis_html_mini
                                        from work.runfull201903041255_analysis_html_mini
                                        ) as tbl;

drop table if exists work.run_201907151224abc;



-- äkki saaks loinci koodid juurde JONIDA, see kiirem kui update
select a.*, e1.t_lyhend, e2.t_lyhend,  e1.loinc_code, e2.loinc_code from work.run_201907151502abcdef as a
    left join (
              select * from work.run_201907151502parameter_name_to_loinc_mapping
              ) as e1
              on a.parameter_name_raw = upper(e1.parameter_name) or
                 a.parameter_name = upper(e1.parameter_name)
    left join (
              select * from work.run_201907151502elabor_parameter_name_to_loinc_mapping
              ) as e2
                on a.parameter_name_raw = upper(e2.parameter_name) or
                   a.parameter_name = upper(e2.parameter_name);

select a.*, e.t_lyhend, e2.t_lyhend,  e.loinc_code, e2.loinc_code from work.run_201907151502abcdef as a
    left join work.run_201907151502parameter_name_to_loinc_mapping as e
                on a.parameter_name_raw = upper(e.parameter_name) or
                   a.parameter_name = upper(e.parameter_name)
    left join work.run_201907151502elabor_parameter_name_to_loinc_mapping as e2
                on a.parameter_name_raw = upper(e2.parameter_name) or
                   a.parameter_name = upper(e2.parameter_name)




select a.*, e.t_lyhend, e2.t_lyhend,  e.loinc_code, e2.loinc_code from work.run_201907151502abcdef as a
    left join (work.run_201907151502parameter_name_to_loinc_mapping as e
                on a.parameter_name_raw = upper(e.parameter_name) or
                   a.parameter_name = upper(e.parameter_name)
    left join work.run_201907151502elabor_parameter_name_to_loinc_mapping as e2
                on a.parameter_name_raw = upper(e2.parameter_name) or
                   a.parameter_name = upper(e2.parameter_name);

-- !!!!!!!!!!!!!
-- tuleb ikka topelt kõiki loinci veerge
select * from work.run_201907151502abcdef as a
    -- mapping using parameter_name
    left join
            (
                select parameter_name, loinc_code, t_lyhend from work.run_201907151502parameter_name_to_loinc_mapping
                union all
                select parameter_name, loinc_code, t_lyhend from work.run_201907151502elabor_parameter_name_to_loinc_mapping
            )
            as name_mappings
    on a.parameter_name_raw = upper(name_mappings.parameter_name) or
        a.parameter_name = upper(name_mappings.parameter_name)
    -- mapping using parameter_name AND parameter_unit
    left join
              (
                  select parameter_name, parameter_unit, loinc_code, t_lyhend
                  from work.run_201907151502parameter_name_parameter_unit_to_loinc_mapping
                  union all
                  select parameter_name, t_yhik as parameter_unit, loinc_code, t_lyhend
                  from work.run_201907151502elabor_parameter_name_parameter_unit_to_loinc_m
              ) as unit_mappings
    on (a.parameter_name_raw = upper(unit_mappings.parameter_name) or
        a.parameter_name = upper(unit_mappings.parameter_name)) and
        upper(a.parameter_unit_raw) = upper(unit_mappings.parameter_unit)


--268 loinci koodiga (mitte null) PN järgi


-- !!!!!!!!!!!!!
-- äkki pole topelt nüüd veerge????
-- vaatab IGA REA puhul PU, mis tihti null....
select * from work.run_201907151502abcdef as a
    -- mapping using parameter_name
    left join
            (
                select parameter_name, parameter_unit, loinc_code, t_lyhend from work.run_201907151502parameter_name_parameter_unit_to_loinc_mapping
                union all
                select parameter_name, t_yhik as parameter_unit, loinc_code, t_lyhend from work.run_201907151502elabor_parameter_name_parameter_unit_to_loinc_m
                union all
                select parameter_name, NULL AS parameter_unit, loinc_code, t_lyhend from work.run_201907151502parameter_name_to_loinc_mapping
                union all
                select parameter_name, NULL AS parameter_unit, loinc_code, t_lyhend from work.run_201907151502elabor_parameter_name_to_loinc_mapping
                )
            as mappings
    on (a.parameter_name_raw = upper(mappings.parameter_name) or
        a.parameter_name = upper(mappings.parameter_name)) and
        upper(a.parameter_unit_raw) = upper(mappings.parameter_unit)





-- teeme testi, kas translateda ö->o on ikka vaja
drop table if exists work.run_201907151502_test2;
create table work.run_201907151502_test2 as
    select * from work.runfull201903041255_analysis_html;

select * from work.run_201907151502_test2 limit 1;

ALTER TABLE work.run_201907151502_test2
add COLUMN loinc_code varchar;

ALTER TABLE work.run_201907151502_test2
add COLUMN elabor_t_lyhend varchar;


select count(*) from work.run_201907151502_test;

CREATE INDEX idx
ON work.run_201907151502_test2 (parameter_name, parameter_name_raw, parameter_unit_raw);

                    update work.run_201907151502_test2 as a
                        set loinc_code = e.loinc_code, elabor_t_lyhend = e.t_lyhend
                        from work.run_201907151502elabor_parameter_name_to_loinc_mapping as e
                        where upper(a.parameter_name_raw) = upper(e.parameter_name) or
                              upper(a.parameter_name) = upper(e.parameter_name);

                    update work.run_201907151502_test2  as a
                        set loinc_code = e.loinc_code,  elabor_t_lyhend = e.t_lyhend
                        from work.run_201907151502elabor_parameter_name_parameter_unit_to_loinc_m e
                        where (upper(a.parameter_name_raw) = upper(e.parameter_name) or
                               upper(a.parameter_name) = upper(e.parameter_name)) and
                            upper(a.parameter_unit_raw) = upper(e.t_yhik);

                    update work.run_201907151502_test2
                            set parameter_name_raw = translate(parameter_name_raw, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U');

                    update work.run_201907151502_test2  as a
                        set loinc_code = p.loinc_code,   elabor_t_lyhend = p.t_lyhend
                        from work.run_201907151502parameter_name_to_loinc_mapping as p
                        where a.loinc_code is null and
                            (upper(a.parameter_name_raw) = upper(p.parameter_name) or
                            upper(a.parameter_name) = upper(p.parameter_name));

                    update work.run_201907151502_test2 as a
                        set loinc_code = p.loinc_code,  elabor_t_lyhend = p.t_lyhend
                        from work.run_201907151502parameter_name_parameter_unit_to_loinc_mapping as p
                        where (upper(a.parameter_name_raw) = upper(p.parameter_name) or
                               upper(a.parameter_name) = upper(p.parameter_name)) and
                               upper(a.parameter_unit_raw) = upper(p.parameter_unit);



select a.*, e1.t_lyhend,  e1.loinc_code from work.run_201907161407cleaned as a
    left join work.run_201907161407elabor_parameter_name_to_loinc_mapping as e1
              on upper(a.parameter_name_raw) = upper(e1.parameter_name) or
                 upper(a.parameter_name) = upper(e1.parameter_name);

drop table if exists work.possible_units;
select * from work.possible_units limit 10;
select * from work.run_201907161407cleaned where parameter_name_raw != parameter_name

/*drop table if exists work.run_201907151502elabor_parameter_name_to_loinc_mapping;
drop table if exists work.run_201907151502elabor_parameter_name_parameter_unit_to_loinc_mapping;
drop table if exists work.run_201907151502elabor_parameter_name_to_loinc_mapping;
drop table if exists work.run_201907151502parameter_unit_to_loinc_unit_mapping;*/



-- Value cleaning VÄGAVÄGA aeglane, üle 40min on 4 000 000 rida

select *,
        work.parameter_name_clean(parameter_name_raw) as parameter_name,
        work.clean_value(value_raw, value_type) as value from
        (
                -- determine value type (float, ratio etc)
                select *, work.check_types(value_raw) as value_type
                from work.runfull201903041255_analysis_html_mini
        ) as tbl

--15.40

set role egcut_epi_work_create;
 create table work.run_201907171241_cleaned as
             select *, parameter_name_raw as parameter_name, value_raw as value from work.runfull201903041255_analysis_html;
reset role;

drop function if exists work.is_float(text text);

set search_path to work;
select * from work.runfull201902260943_analysis_html limit 1;


--create table run_201907221120_test as
(select *,
        parameter_name_clean(parameter_name_raw) as parameter_name,
        clean_value(value_raw, value_type) as value from
        (
                -- determine value type (float, ratio etc)
                select *, check_types(value_raw) as value_type
                from runfull201902260943_analysis_html
                limit 1
        ) as tbl);

