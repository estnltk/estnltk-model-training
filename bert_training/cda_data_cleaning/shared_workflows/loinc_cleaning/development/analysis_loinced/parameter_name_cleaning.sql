---------------------------
-- Cleaning parameter_name
---------------------------
create or replace function analysiscleaning.parameter_name_clean(parameter_name text)
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
    if(parameter_name_clean ~ '^[a][0-9]{4}') then parameter_name_clean := regexp_replace(parameter_name_clean, '^[a][0-9]{4}','');end if;
    if(parameter_name_clean ~ '^[a][0-9]{3}') then parameter_name_clean := regexp_replace(parameter_name_clean, '^[a][0-9]{3}','');end if;
    if(parameter_name_clean ~ '^[a][0-9]{2}') then parameter_name_clean := regexp_replace(parameter_name_clean, '^[a][0-9]{2}','');end if;
    if(parameter_name_clean ~ '^[a][0-9]{1}') then parameter_name_clean := regexp_replace(parameter_name_clean, '^[a][0-9]{1}','');end if;


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

select * from analysiscleaning.parameter_name_clean('a1278#wbc*');

drop table if exists analysiscleaning.test;

SET ROLE egcut_epi_work_create;
create table work.ztest as select * from work.runfull201903041255_analysis_html where epi_id = '1281432';
reset role;

SET ROLE egcut_epi_work_create;
create table work.p2test as select parameter_name_raw from work.runfull201903041255_analysis_html where epi_id = '1281432';
reset role;
select * from work.p2test;

create table work.zzztest_loinced as
    select row_nr, epi_id, epi_type, parse_type, panel_id, analysis_name_raw, parameter_name_raw,
                               parameter_unit_raw, reference_values_raw, effective_time_raw, value_raw
                         from work.zzztest;


select * from work.runfull201903041255_analysis_html
where parameter_name_raw like '\#%';

alter table work.zzztest
add column parameter_name_clean varchar;
select *
from work.zzztest;

--!!!!
update work.zzztest
set parameter_name_clean = analysiscleaning.parameter_name_clean(parameter_name_raw);
select * from work.zzztest;


select analysiscleaning.parameter_name_clean(parameter_name_raw), * from work.zzztest

SELECT epi_id FROM work.zzztest.columns
WHERE table_name=zzztest and column_name='epi_id';

EXPLAIN SELECT count(*) > 0 FROM information_schema.columns
WHERE table_schema LIKE 'us_ut_sor' AND table_name LIKE 'dirtyrecords' AND
      column_name LIKE 'bullage'

explain select count(*) > 0 from work.zzztest
where table_schema


SELECT column_name FROM information_schema.columns
WHERE table_name='your_table' and column_name='your_column';

SELECT EXISTS (
    SELECT *
FROM work.zzztest
WHERE parameter_name_raw='parameter_name_raw');

SELECT * FROM zzztest.columns; -- WHERE table_name = tableName AND column_name = columnName)


-- !!!!!!!!
select column_name from information_schema.columns where table_schema = 'work' and table_name = 'zzztest';

--!! peaaegu
select * from information_schema.columns where table_schema = 'work' and table_name = 'zzztest' and
                                                         (column_name = 'epi_idd' or column_name = 'row_nr');

select 1
from work.zzztest
where
    (select column_name from information_schema.columns where table_schema = 'work' and table_name = 'zzztest')  = 'epi_id';

select * from
(select column_name from information_schema.columns where table_schema = 'work' and table_name = 'zzztest') as t1
inner join
(select 'epi_id', 'row_nr') as t2;




drop function if exists analysiscleaning.exist_columns(source_schema text, source_table text);
create or replace function analysiscleaning.exist_columns(source_schema text, source_table text)
    returns boolean
as
$body$
declare
    columns text[] := (select array_agg(column_name) from information_schema.columns
                        where table_schema = source_schema and table_name = source_table);
begin
    --required columns
    if ('row_nr') = any(columns) and
       ('epi_id') = any(columns) and
       ('epi_type') = any(columns) and
       ('parse_type') = any(columns) and
       ('panel_id') = any(columns) and
       ('analysis_name_raw') = any(columns) and
       ('parameter_name_raw') = any(columns) and
       ('parameter_unit_raw') = any(columns) and
       ('reference_values_raw') = any(columns) and
       ('effective_time_raw') = any(columns) and
       ('value_raw') = any(columns) and
       ('analysis_substrate_raw') = any(columns)
        then return TRUE;
    end if;
    return FALSE;
end
$body$
language plpgsql;

--select array_agg(column_name) from information_schema.columns where table_schema = 'work' and table_name = 'zzztest';
--select analysiscleaning.exist_columns()
--select analysiscleaning.exist_columns('{row_nr,random, epi_id,epi_type,parse_type,panel_id,analysis_name_raw,parameter_name_raw,parameter_unit_raw,reference_values_raw,effective_time_raw,value_raw,analysis_substrate_raw}');
--select analysiscleaning.exist_columns('work', 'zzztest');y
select work.exist_columns('work', 'yyytest');
select work.exist_columns('work', 'p3test');

SET ROLE egcut_epi_work_create;
create table work.p11test as select parameter_name_raw from work.runfull201903041255_analysis_html where epi_id = '1281432';
reset role;

create table work.p11test
    (add column uus varchar)
as select parameter_name_raw, epi_id from work.runfull201903041255_analysis_html where epi_id = '1281432';


CREATE TABLE work.p11test
(
 additional_field1 varchar
) select parameter_name_raw, epi_id from work.runfull201903041255_analysis_html where epi_id = '1281432'
;


CREATE TABLE new_tbl (
  extra_col VARCHAR(64)
) SELECT * FROM orig_tbl


CREATE TABLE DOC(
    ID INT PRIMARY KEY,
    STATUS INT,
    REMINDERINFORMATION VARCHAR(255)
)
AS SELECT I.ID, I.STATUS, A.REMINDERINFORMATION
FROM IE802 I JOIN IE802_ATTRIBUTES A ON A.IE802_ID=I.ID;



select * from work.zzztest where column_name in (
                                select column_name from information_schema.columns
                                where table_schema = 'work' and table_name = 'zzztest')


SELECT exists
    (
    SELECT 1 FROM
        (select column_name from information_schema.columns where table_schema = 'work' and table_name = 'zzztest') as tbl
    WHERE column_name = 'row_nr' and column_name = 'epi_id' LIMIT 1);

select * from work.zzztest where
('row_nr', 'epi_id') in
(select column_name from information_schema.columns where table_schema = 'work' and table_name = 'zzztest') ;

SELECT '{epi_id}'   && {(select column_name from information_schema.columns where table_schema = 'work' and table_name = 'zzztest')}::varchar[];

--!!!!
SELECT column_name = ANY ('{epi_id, row_nr}'::varchar[])
    from (select column_name from information_schema.columns where table_schema = 'work' and table_name = 'zzztest') as t


SELECT column_name = ANY ('{row_nr,epi_id,epi_type
parse_type,
panel_id,
analysis_name_raw,
parameter_name_raw,
parameter_unit_raw,
reference_values_raw,
effective_time_raw,
value_raw,
analysis_substrate_raw
}'::varchar[]) as olemas
    -- tabelis olemas olevad veerud
    from (select column_name from information_schema.columns
            where table_schema = 'work' and table_name = 'zzztest') as t;




select *
from (
   values ('row_nr'),('epiii_id')
) as val
where exists ((select column_name from information_schema.columns where table_schema = 'work' and table_name = 'zzztest' and
                                                                        column_name=val) );




SELECT *
FROM information_schema.columns
WHERE table_schema = 'your_schema'
  AND table_name   = 'your_table'


select abc from work.zzztest;


select row_nr, epi_id, epi_type, parse_type, panel_id, analysis_name_raw, parameter_name_raw,
                               parameter_unit_raw, reference_values_raw, effective_time_raw, value_raw
                        from work.zzztest


select * from classifications.elabor_analysis
where upper(kasutatav_nimetus) like 'PIKKUS';


select * from classifications.elabor_analysis
where loinc_no like '29463-7';