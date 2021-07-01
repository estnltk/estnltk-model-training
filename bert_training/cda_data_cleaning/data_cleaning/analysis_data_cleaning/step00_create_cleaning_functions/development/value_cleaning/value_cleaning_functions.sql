-- common.value
-- no cleaning for text!

drop function if exists analysiscleaning.clean_floats(value text);
drop function if exists analysiscleaning.match_float(text text);
drop function if exists analysiscleaning.is_float(text text);


select analysiscleaning.clean_floats('.');
select analysiscleaning.match_float('.8');
select analysiscleaning.is_float('.8');
select analysiscleaning.remove_units('8', True);


-----------------------------------------------------------------
-- CLEAN FLOATS
-----------------------------------------------------------------

create or replace function work.clean_floats (value text)
    returns text
as
$body$
declare
    part0 text;
    part1 text;
    clean text;
begin

    value := trim(both from analysiscleaning.remove_units(value, FALSE)); --removing whitespaces from begin and end
    value := regexp_replace(value, ',','.');
    part0 :=  split_part(value, '.', 1);    --split to two: BEFORE and after dot
    part1 :=  split_part(value, '.', 2);    --split to two: before and AFTER dot

    if (substr(part0,1,1) = '-') then clean := '-'; end if;  -- does value start with minus

    part0 := replace(translate(part0,'+-',' '),' ','');  -- remove all '-' and '+' from beginning
    part0 := trim(both from part0);
    part1 := trim(both from part1);

    if (char_length(part0) = 0) then part0 := '0'; end if;-- kui .9 siis 0.9
    if (char_length(part1) = 0) then part1 := '0'; end if;-- kui 4. siis 4.0

    clean := concat(clean, trim(both from part0), '.', trim(both from part1));
    return clean;
end
$body$
language plpgsql;



select analysiscleaning.clean_floats('.');
select analysiscleaning.clean_floats('.8');
select analysiscleaning.clean_floats('8.');
select analysiscleaning.clean_floats('-.8');
select analysiscleaning.clean_floats('-8.');
select analysiscleaning.clean_floats('+.8');
select analysiscleaning.clean_floats('+8.');
select analysiscleaning.clean_floats('+-8.');


-----------------------------------------------------------------
--REMOVE UNITS
-----------------------------------------------------------------

-- NB! assumption that possible units table exists in analysiscleaning
-- tekst on '9.810g/l', (tegelikult on '9.8 10g/l', teine osa on unit)
-- strict == False: ALWAYS remove unit
        --  if '9.810g/l' then '9.8'
-- strict == True: only removes unit when whitespace inbetween
        -- if '9.810g/l', then '9.810g/l',
        -- if '9.8 10g/l', then '9.8',
create or replace function work.remove_units (rawtext text, strict boolean)
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

    -- is unit the last element in text?
    for pos_unit in select * from possible_units loop

        -- unit '%' is special character and needs to be escaped
        if (pos_unit like '%\%%') then pos_unit := regexp_replace(pos_unit, '%', '\%'); end if;

        if (lower(last_element) = lower(pos_unit)     and length(pos_unit) > length(selected_unit) and strict)  or-- last element of text is unit
           (lower(text) like ('%' || lower(pos_unit)) and length(pos_unit) > length(selected_unit) and not strict) -- text endswith unit
            then selected_unit := pos_unit;
        end if;
    end loop;

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

select work.remove_units('20 - 40 %', FALSE);


--concat('.*', unit, '$')
select '3:4g/l' like '.*g/l$';
select '3:4%' like concat('.*', '%', '$');
select '3:4g/l' ~ ('' || 'g/l' || '$');
select regexp_matches('3:4g/l', '.*' || 'g/l' || '$');
select regexp_replace('345%', '%', '\%');

select '3/4' like concat('%', '\%')

select * from analysiscleaning.possible_units where unit like '%0%'

--strict doesnt remove unit if there is no whitespace in between
select * from analysiscleaning.remove_units('140', FALSE);
select * from analysiscleaning.remove_units('140', TRUE);

select * from analysiscleaning.remove_units('345%', FALSE);
select * from analysiscleaning.remove_units('345%', TRUE);

select * from analysiscleaning.remove_units('345 %', FALSE);
select * from analysiscleaning.remove_units('345 %', TRUE); --ppp

select * from analysiscleaning.remove_units('345%0', FALSE);
select * from analysiscleaning.remove_units('345%0', TRUE);

select * from analysiscleaning.remove_units('3/4', FALSE);
select * from analysiscleaning.remove_units('3/4', TRUE);

select * from analysiscleaning.remove_units('9.8 10g/l', FALSE);
select * from analysiscleaning.remove_units(' 9.8 10g/l ', TRUE);

select * from analysiscleaning.remove_units('9.810g/l', TRUE);
select * from analysiscleaning.remove_units('9.810g/l', FALSE);


-----------------------------------------------------------------
-- MATCH FLOAT
-----------------------------------------------------------------

create or replace function work.match_float (text text)
    returns boolean
as
$body$
declare
    match_float boolean := False;
begin
    -- matching float
    if text ~ '^\s*[+-]?[0-9][\.,][0-9]*\s*$' or
       text ~ '^\s*[+-]?[\.,][0-9]+\s*$'or
       text ~ '^\s*[+-]?[1-9][0-9]*[\.,][0-9]*\s*$'
    then match_float := True;
    end if;
    return match_float;
end
$body$
language plpgsql;

select * from analysiscleaning.match_float('2.3aa');
select * from analysiscleaning.match_float('2.3');
select * from analysiscleaning.match_float('2');
select * from analysiscleaning.match_float('+2.3');
select * from analysiscleaning.match_float('+2');

-----------------------------------------------------------------
-- IS FLOAT
-----------------------------------------------------------------
drop function if exists work.is_float(text text);
create or replace function work.is_float (text text)
    returns boolean
as
$body$
declare
begin
    if work.match_float(work.remove_units(text, True)) --code in common.value has default strict = True
        then
        return True;
        -- listid puhastatud väärtustest ja tüübist, peaksid olema üleüldised nagu self.xxx
        --clean := array_append(clean, analysiscleaning.clean_floats(analysiscleaning.remove_units(text)))
        --type := array_append(type, 'float')
        end if;
    return False;
end
$body$
language plpgsql;

set search_path to work;
drop table if exists work.possible_units ;
select * from work.possible_units limit 2;
select * from work.is_float('4,5'); -- true,
select * from work.is_float('4.5'); -- true
select * from analysiscleaning.is_float('4.5a'); -- false (true, kui strict = False....)
select * from analysiscleaning.is_float('4.5ab'); -- false
select * from analysiscleaning.is_float('a4.5'); -- false
select * from analysiscleaning.is_float('-4.5'); -- true
select * from analysiscleaning.is_float('+4.5'); -- true

select * from analysiscleaning.is_float('4.5 10g/l'); -- true
select * from analysiscleaning.is_float('4.510g/l'); -- false... (True, kui strict = False)
select * from analysiscleaning.is_float('4 10g/l'); -- false

SELECT *
  FROM pg_locks l
  JOIN pg_class t ON l.relation = t.oid AND t.relkind = 'r'
 WHERE t.relname = 'possible_units';

SELECT pid, query FROM pg_stat_activity;
SELECT pg_terminate_backend(12722);
5465
5463
753
30345
26696
27795
27255
23268
10882
19707
22217
17134
14452
14419
6887
5895
11922
4348
5410
4334
31781
14884
13863
8164
13858
11472
11352
3837
28569
28245
28242
25602
24650
23460
23078
20497
18360
17979
15311
10329
7788
6890
7780
5812
5296
2778
12329
11019
16959
11802
3084
1832
31728
31714
29223
29218
25294
26697
16591
16548
3355
11887
31390
31271
27421
26215
23734
12722
20392
22887
3354
15507
22883
22881
22882
22884

--------------------------------------------------------------------------------------
--CLEAN INTEGER
--------------------------------------------------------------------------------------
create or replace function analysiscleaning.clean_integer (value text)
    returns text
as
$body$
declare
    clean text;
begin
    value := trim(both from work.remove_units(value, FALSE)); --removing whitespace from begin and end

    if (substr(value,1,1) = '-') then clean := '-'; end if;  -- if number startswith '-' keep the '-'

    value := replace(translate(value,'+-',' '),' ','');  --remove all the '+' and '-'

    return concat(clean, trim(both from value));
end
$body$
language plpgsql;

select * from analysiscleaning.clean_integer('140');

select * from analysiscleaning.clean_integer('4'); -- 4
select * from analysiscleaning.clean_integer('4.5'); -- 4.5
select * from analysiscleaning.clean_integer('-4'); -- -4
select * from analysiscleaning.clean_integer('+4'); -- 4
select * from analysiscleaning.clean_integer('+-4'); -- 4
select * from analysiscleaning.clean_integer(' 4'); -- 4
select * from analysiscleaning.clean_integer('4 '); -- 4
select * from analysiscleaning.clean_integer(' 4 '); -- 4

--------------------------------------------------------------------------------------
--MATCH INTEGER
--------------------------------------------------------------------------------------
create or replace function work.match_integer (text text)
    returns boolean
as
$body$
declare
    match_integer boolean := False;
begin
    if text ~ '^\s*[+-]?[1-9][0-9]*\s*$' or
       text ~ '^\s*0\s*$'
    then match_integer := True;
    end if;

    return match_integer;
end
$body$
language plpgsql;


select * from analysiscleaning.match_integer('4'); -- true
select * from analysiscleaning.match_integer('4.5'); -- false
select * from analysiscleaning.match_integer('4,5'); -- false
select * from analysiscleaning.match_integer('4a'); -- false
select * from analysiscleaning.match_integer('a4'); -- false
select * from analysiscleaning.match_integer('-4'); -- true
select * from analysiscleaning.match_integer('+4'); -- true







--------------------------------------------------------------------------------------
--IS INTEGER
--------------------------------------------------------------------------------------
create or replace function work.is_integer (text text)
    returns boolean
as
$body$
declare

begin
    if work.match_integer(work.remove_units(text, TRUE))
        then return True;
        -- listid puhastatud väärtustest ja tüübist, peaksid olema üleüldised nagu self.xxx
        --clean := array_append(clean, analysiscleaning.clean_integer(analysiscleaning.remove_units(text)))
        --type := array_append(type, 'integer')
        end if;
    return False;
end
$body$
language plpgsql;

--!!
select * from analysiscleaning.is_integer('4'); -- true
select * from analysiscleaning.is_integer('4.5'); -- false
select * from analysiscleaning.is_integer('4,5'); -- false
select * from analysiscleaning.is_integer('4a'); -- false
select * from analysiscleaning.is_integer('a4'); -- false
select * from analysiscleaning.is_integer('-4 '); -- true
select * from analysiscleaning.is_integer('+4'); -- true

select * from analysiscleaning.is_integer('4.5 10g/l'); --false
select * from analysiscleaning.is_integer('4 10g/l'); --true
select * from analysiscleaning.is_integer('410g/l'); --false





-------------------------------------------------------------------------------------
-- MATCH TEXT
-------------------------------------------------------------------------------------

-- string is considered text if it contains only characters
create or replace function analysiscleaning.match_text (text text)
    returns boolean
as
$body$
declare
    match_text boolean := False;
begin
    -- matching float
    if text ~ '^\s*[^0-9]+\s*$' then match_text := True;
    end if;
    return match_text;
end
$body$
language plpgsql;


select * from analysiscleaning.match_text('abc'); -- true
select * from analysiscleaning.match_text('4'); -- false
select * from analysiscleaning.match_text('4a'); -- false
select * from analysiscleaning.match_text('a4'); -- false
select * from analysiscleaning.match_text('10g/l'); -- false



-------------------------------------------------------------------------------------
-- IS TEXT
-------------------------------------------------------------------------------------

create or replace function analysiscleaning.is_text (text text)
    returns boolean
as
$body$
declare
begin
    if analysiscleaning.match_text(analysiscleaning.remove_units(text, TRUE))
        then
        return True;
        -- listid puhastatud väärtustest ja tüübist
        --clean := array_append(clean, None)
        --type := array_append(type, 'text')
        end if;
    return False;
end
$body$
language plpgsql;


select * from analysiscleaning.is_text('aaa'); -- true
select * from analysiscleaning.is_text('aaa 10g/l'); -- true
select * from analysiscleaning.is_text('a4a 10g/l'); -- false




-------------------------------------------------------------------------------
--MATCH NUM AND PAR (parentheses)
-------------------------------------------------------------------------------
--deals with text that contains parentheses

drop function if exists analysiscleaning.match_num_and_par(text text);
create or replace function analysiscleaning.match_num_and_par (text text)
    returns text
as
$body$
declare
    part0 text;
    part1 text;
    type_p0 text;
    type_p1 text;
    clean_p0 text;
    clean_p1 text;
begin
    text := analysiscleaning.remove_units(text, FALSE);
    part0 := trim(both from split_part(text, '(', 1));
    part1 := trim(both from split_part(text, '(', 2));

    if length(part1) != 0 and part1 like '%)' then
        part1 := rtrim(part1,')');

        -- determines types (float or integer) of part0 and part1
        if analysiscleaning.match_integer(part0) then
            select 'integer', analysiscleaning.clean_integer(part0) into  type_p0, clean_p0;
        elseif analysiscleaning.match_float(part0) then
            select 'float',   analysiscleaning.clean_floats(part0) into  type_p0, clean_p0;
        end if;

        if analysiscleaning.match_integer(part1) then
            select 'integer', analysiscleaning.clean_integer(part1) into  type_p1, clean_p1;
        elseif analysiscleaning.match_float(part1) then
            select 'float',   analysiscleaning.clean_floats(part1) into  type_p1, clean_p1;
        end if;


        -- determines if part0 and part1 are the same
        if type_p0 = 'float' and type_p1 = 'float' then
            if clean_p0 = clean_p1 then
             return  clean_p0;
            end if;
        elseif type_p0 = 'float' and type_p1 = 'integer' then
            if clean_p0::float = clean_p1::float then
                return clean_p0;
            end if;

        elseif type_p0 = 'integer' and type_p1 = 'float' then
            if clean_p0::float = clean_p1::float then
                return clean_p1;
            end if;

        elseif type_p0 = 'integer' and type_p1 = 'integer' then
            if clean_p0 = clean_p1 then
                return clean_p0;
            end if;

        end if;
    end if;
    return False;
end
$body$
language plpgsql;

select * from analysiscleaning.match_num_and_par('4.0(4.0)'); --4.0
select * from analysiscleaning.match_num_and_par('4(4.0)'); --4.0
select * from analysiscleaning.match_num_and_par('4.0(4)'); --4.0
select * from analysiscleaning.match_num_and_par('4(4)'); -- 4
select * from analysiscleaning.match_num_and_par('4(5)'); -- False




-------------------------------------------------------------------------------
--IS NUM AND PAR (parentheses)
-------------------------------------------------------------------------------

drop function if exists analysiscleaning.is_num_and_par(text text);
create or replace function analysiscleaning.is_num_and_par (text text)
    returns boolean
as
$body$
declare
    is_par text;
    katse text;
begin
    katse := analysiscleaning.remove_units(text, False);
    is_par :=  analysiscleaning.match_num_and_par(analysiscleaning.remove_units(text, True));
    -- paranthesis is a number
    if is_par ~ '^[0-9\.]+$' then
        -- listid puhastatud väärtustest ja tüübist
        --clean := array_append(clean, m)
        --type := array_append(type, 'num_and_par')
        return True;
        end if;
    return False;
end
$body$
language plpgsql;


select * from analysiscleaning.is_num_and_par('.1(.1)'); -- true
select * from analysiscleaning.is_num_and_par('1.(1.)'); -- true
select * from analysiscleaning.is_num_and_par('9.9(9.9)'); -- true
select * from analysiscleaning.is_num_and_par('1(2)'); --false
select * from analysiscleaning.is_num_and_par('a(a)'); -- false
select * from analysiscleaning.is_num_and_par('1'); -- false
select * from analysiscleaning.is_num_and_par('(1)'); -- false


-------------------------------------------------------------------------------
-- MATCH RATIO
-------------------------------------------------------------------------------

drop function if exists analysiscleaning.match_ratio;
create or replace function analysiscleaning.match_ratio (text text)
    returns text
as
$body$
declare
    part0 text;
    part1 text;
    part2 text;
    clean_p1 text;
    clean_p2 text;
    clean text;
begin
    text := analysiscleaning.remove_units(text, FALSE);
    text := regexp_replace(text, ':','/');
    part2 := split_part(text, '/',3);

    --has exactly 2 parts
    if part2 = '' then
        part0 := trim(both from split_part(text, '/', 1));    --split to two: BEFORE and after '('
        part1 := trim(both from split_part(text, '/', 2));    --split to two: before and AFTER '('
        if analysiscleaning.match_integer(part0) and analysiscleaning.match_integer(part1) then
            clean_p1 := analysiscleaning.clean_integer(part0);
            clean_p2 := analysiscleaning.clean_integer(part1);
            clean := concat(clean_p1, '/', clean_p2);
            return clean;
        end if;
    end if;
    return NULL;
end
$body$
language plpgsql;

select * from analysiscleaning.match_ratio('3:4'); -- 3/4
select * from analysiscleaning.match_ratio('3:4 10g/l'); -- 3/4
select * from analysiscleaning.match_ratio('3/4'); -- 3/4
select * from analysiscleaning.match_ratio('3:4.5'); --null
select * from analysiscleaning.match_ratio('123'); -- null


-------------------------------------------------------------------------------
-- IS RATIO
-------------------------------------------------------------------------------

drop function if exists analysiscleaning.is_ratio(text text);
create or replace function analysiscleaning.is_ratio (text text)
    returns boolean
as
$body$
declare
    m text := analysiscleaning.match_ratio(analysiscleaning.remove_units(text, FALSE));
begin
    if m is not NULL then return TRUE;
    end if;
    return FALSE;
end
$body$
language plpgsql;

select * from analysiscleaning.is_ratio('3:4g/L'); -- true
select * from analysiscleaning.is_ratio('3/4g/L'); -- true
select * from analysiscleaning.is_ratio('3.4g/L'); -- false
select * from analysiscleaning.is_ratio('3:4'); --true

-------------------------------------------------------------------------------
-- CLEAN RANGE
-------------------------------------------------------------------------------

create or replace function analysiscleaning.clean_range (text text, start text)
    returns text
as
$body$
declare
    par1    text := '(';
    par2    text := ')';
    min_val text := '-Inf';
    max_val text := 'Inf';
begin
    if start like '>%' then
        min_val := text;
        if start like '%=%' then
            par1 := '[';
        end if;
    end if;

    if start like '<%' then
        max_val := text;
        if start like '%=%' then
            par2 := ']';
        end if;
    end if;

    return concat(par1, min_val, ';', max_val, par2);

end
$body$
language plpgsql;

select * from analysiscleaning.clean_range('3', '<='); --(-Inf;3]
select * from analysiscleaning.clean_range('3', '<'); -- (-Inf;3)
select * from analysiscleaning.clean_range('3', '>='); -- [3;Inf)
select * from analysiscleaning.clean_range('3', '>'); -- (3;Inf)





-------------------------------------------------------------------------------
-- MATCH_GREATER_LESS TODO
-------------------------------------------------------------------------------

drop function if exists analysiscleaning.match_greater_less(text text);
create or replace function analysiscleaning.match_greater_less (text text)
    returns text
as
$body$
declare
    start text;
    starts text[] := array['>=', '<=', '>', '<'];
    value text;
    clean text;
begin
    for i in 1..4 loop
        start := starts[i];
         -- kas text algab >=, <=, >, < märgiga
        if (text like concat(start, '%') ) and ((char_length(text)) > char_length(start))
           then text := TRANSLATE(text, start, '');
                value := null;

                if analysiscleaning.match_float(text) then value := analysiscleaning.clean_floats(text); end if;
                if analysiscleaning.match_integer(text) then value := analysiscleaning.clean_integer(text); end if;

                if value is not null
                    then clean := analysiscleaning.clean_range(value, start);
                        return clean;
                end if;
        end if;
    end loop;
    return NULL;
end
$body$
language plpgsql;

select * from analysiscleaning.match_greater_less('<3.9'); --  (-Inf;3.9)
select * from analysiscleaning.match_greater_less('<=3.9'); --  (-Inf;3.9]
select * from analysiscleaning.match_greater_less('>3.9'); --  (3.9;Inf)
select * from analysiscleaning.match_greater_less('>=3.9'); --  [3.9;Inf)
select * from analysiscleaning.match_greater_less('>=3.9a'); -- null
select * from analysiscleaning.match_greater_less('3.9'); -- null
select * from analysiscleaning.match_greater_less('>='); --  null




-------------------------------------------------------------------------------
-- MATCH_BETWEEN TODO
-------------------------------------------------------------------------------

drop function if exists analysiscleaning.match_between(text text);
create or replace function analysiscleaning.match_between (text text)
    returns text
as
$body$
declare
    val1 text := null;
    val2 text := null;

    text text := trim(both from text);
    part1 text :=  analysiscleaning.remove_units(trim(both from split_part(text, '-', 1)), TRUE);
    part2 text :=  analysiscleaning.remove_units(trim(both from split_part(text, '-', 2)), TRUE);
    part3 text :=  analysiscleaning.remove_units(trim(both from split_part(text, '-', 3)), TRUE);
    part4 text :=  analysiscleaning.remove_units(trim(both from split_part(text, '-', 4)), TRUE);

    min_val text := null;
    max_val text := null;

begin

    -- negative range elements
    if part1 = '' and part3 = ''
       then val1 := concat('-', part2);
            val2 := concat('-', part4);
    end if;

    -- one negative one positive range element
    if part1 = '' and part4 = ''
       then val1 := concat('-', part2);
            val2 := part3;
    end if;

    -- positive range elements
    if part2 != '' and part3 = '' and part4 = ''
       then val1 := part1;
            val2 := part2;
    end if;

    if analysiscleaning.match_integer(val1) then min_val = analysiscleaning.clean_integer(val1); end if;
    if analysiscleaning.match_float(val1) then min_val = analysiscleaning.clean_floats(val1); end if;
    if analysiscleaning.match_integer(val2) then max_val = analysiscleaning.clean_integer(val2); end if;
    if analysiscleaning.match_float(val2) then max_val = analysiscleaning.clean_floats(val2); end if;

    if min_val is not null and max_val is not null and cast(max_val as float) > cast(min_val as float) then
        return concat('[',min_val, ',', max_val,']');
    end if;

    return NULL;
end
$body$
language plpgsql;

select * from analysiscleaning.match_between('2-4'); --true [2,4]
select * from analysiscleaning.match_between('-2-4'); -- true [-2, 4]
select * from analysiscleaning.match_between('-2--4');--false null
select * from analysiscleaning.match_between('2--4');--false null
select * from analysiscleaning.match_between('24');--false null

select * from analysiscleaning.match_between('2.1-4.1'); --true [2.1,4.1]
select * from analysiscleaning.match_between('-2.1-4.1'); -- true [-2.1,4.1]
select * from analysiscleaning.match_between('-2.1--4.1');--false null
select * from analysiscleaning.match_between('2.1--4.1');--false null

select * from analysiscleaning.match_between(' 2 - 4 '); --true [2,4]
select * from analysiscleaning.match_between(' - 2 - 4 '); -- true [-2,4]
select * from analysiscleaning.match_between('- 2 - - 4 ');--false null
select * from analysiscleaning.match_between(' 2 - - 4');--false null

select distinct reference_values_raw from analysiscleaning.runfull201903041255_analysis_html_loinced
where  reference_values_raw like '%- -%'
limit 100;




-------------------------------------------------------------------------------
-- MATCH RANGE WITH DOTS
-------------------------------------------------------------------------------

create or replace function work.match_range_with_dots (text text)
    returns text
as
$body$
declare
    text text := trim(both from text);
    part1 text :=  remove_units(trim(both from split_part(text, '..', 1)), TRUE);
    part2 text :=  remove_units(trim(both from split_part(text, '..', 2)), TRUE);
begin
    if (is_float(part1) and is_float(part2) and clean_floats(part1) < clean_floats(part2)) or
       (is_integer(part1) and is_integer(part2) and clean_integer(part1) < clean_integer(part2)) then
        return concat('[',part1, ',', part2,']');
    end if;
    return NULL;
end
$body$
language plpgsql;

select work.match_range_with_dots('12..31');
select match_range_with_dots('3,5...5,5');
select clean_floats('3,5');


/*
   def is_range_with_dots(self):
        parts = self.units_removed.split("...")
        if len(parts) == 2:
            try:
                val1 = int(parts[0])
                val2 = int(parts[1])
                if val1 < val2:
                    self.clean.append("[" + str(val1) + ";" + str(val2) + "]")
                    self.type.append("range")
                    return True
            except ValueError as e:
                return False
        return False
*/


-------------------------------------------------------------------------------
-- MATCH_RANGE
-------------------------------------------------------------------------------
drop function if exists match_range(text text);
create or replace function match_range (text text)
    returns text
as
$body$
declare
    text text := rtrim(remove_units(trim(both from text), FALSE), 'x'); --eemaldab x: 150 - 400 x 10*9/l -> 150 - 400
    --greater_less text := analysiscleaning.match_greater_less(text);
    --between_range text := analysiscleaning.match_between(text);
    --dots text := match_range_with_dots(text);
begin
    if match_greater_less(text) is not NULL then return match_greater_less(text); end if;
    if match_between(text) is not NULL then return match_between(text); end if;
    if match_range_with_dots(text) is not NULL then return match_range_with_dots(text); end if;
    return NULL;
end
$body$
language plpgsql;

select match_range('4.0..9.9');
select * from possible_units
where unit like '10e9%';

/*    def is_with_x(self):
        if self.units_removed.lower().endswith('x'):
            value = self.units_removed.strip('x').strip('X')
            try:
                result = int(value)
                self.clean.append(str(result))
                self.type.append('integer')
                return True
            except ValueError as e:
                return False
        return False
*/


select * from analysiscleaning.match_range('<3.9'); --(-Inf;3.9)
select * from analysiscleaning.match_range('-2-4'); -- [-2,4]
select * from analysiscleaning.match_range('24'); --NULL

select RTRIM('150 - 400 x', 'x');
/*
vaja puhastada: 150 - 400 x 10*9/l
    def match_range(self, text):
        text = text.strip()
        greater_less = self.match_greater_less(text)
        if greater_less:
            return ["range", str(greater_less)]
        between = self.match_between(text)
        if between:
            return ["range", str(between)]
        return False

    def is_range(self):
        m = self.match_range(self.units_removed)
        if m:
            self.clean.append(m[1])
            self.type.append(m[0])
            return True
        return False
*/

-------------------------------------------------------------------------------
-- IS RANGE
-------------------------------------------------------------------------------
drop function if exists analysiscleaning.is_range(text text);
create or replace function analysiscleaning.is_range (text text)
    returns boolean
as
$body$
declare
    m text := analysiscleaning.match_range(analysiscleaning.remove_units(text, TRUE));
begin
    if m is not NULL then
     -- self.clean.append(m[1])
     -- self.type.append(m[0])
        return TRUE;
    end if;
    return FALSE;
end
$body$
language plpgsql;


select * from analysiscleaning.is_range('9.8 10g/l-19.8 10g/l');
select * from analysiscleaning.is_range('-2-4');


-------------------------------------------------------------------------------------
--CHECK TYPES
-------------------------------------------------------------------------------------
create or replace function analysiscleaning.check_types(text text)
    returns text
as
$body$
declare
begin
    if analysiscleaning.is_float(text) then return 'float'; end if;
    if analysiscleaning.is_integer(text) then return 'integer'; end if;
    if analysiscleaning.is_text(text) then return 'text'; end if;
    if analysiscleaning.is_num_and_par(text) then return 'num_and_par'; end if;
    if analysiscleaning.is_ratio(text) then return 'ratio'; end if;
    if analysiscleaning.is_range(text) then return 'range'; end if;
    return NULL;

end
$body$
language plpgsql;


select * from analysiscleaning.check_types('4.0'); --float
select * from analysiscleaning.check_types('4'); -- integer
select * from analysiscleaning.check_types('negatiivne'); --text
select * from analysiscleaning.check_types('4(4)'); --num_and_par
select * from analysiscleaning.check_types('<=3.9'); -- range
select * from analysiscleaning.check_types('-2-4'); -- range
select * from analysiscleaning.check_types('3:4'); --ratio
select * from analysiscleaning.check_types('3/4'); --ratio

-------------------------------------------------------------------------------------
-- CLEAN VALUE according to value type
-------------------------------------------------------------------------------------
create or replace function analysiscleaning.clean_value(value_raw text, type text)
    returns text
as
$body$
declare
    clean text;
begin
    if type = 'float' then clean := analysiscleaning.clean_floats(value_raw); end if;
    if type = 'integer' then clean := analysiscleaning.clean_integer(value_raw); end if;
    if type = 'text' then clean := value_raw; end if; --don't know how to clean text
    if type = 'num_and_par' then clean := analysiscleaning.match_num_and_par(value_raw); end if;
    if type = 'ratio' then clean := analysiscleaning.match_ratio(value_raw); end if;
    if type = 'range' then clean := analysiscleaning.match_range(value_raw); end if;
    return clean;

end
$body$
language plpgsql;

--select * from analysiscleaning.clean_value('3:4%', 'ratio');
select * from analysiscleaning.clean_value('3,4 g/l', 'float'); --3.4
select * from analysiscleaning.clean_value('+3 g/l', 'integer'); --3
select * from analysiscleaning.clean_value('3.0(3) g/l', 'num_and_par'); --3.0
select * from analysiscleaning.clean_value('3:4 g/l', 'ratio'); --3/4
select * from analysiscleaning.clean_value('<=3', 'range'); -- (-Inf;3)
select * from analysiscleaning.clean_value('-3-4', 'range'); -- [-3,4]


