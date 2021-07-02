--assumption - exists a table with loinc_code
drop table if exists analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency;

--kopeerime parameter_unit_raw uude veergu loinc_unit
create table analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as
  select parameter_unit_raw as loinc_unit,  a.*
  from analysiscleaning.analysis_html_loinced2_runfull201904041255 as a;

-- paneme külge loinci_unitid parameter_unit -> loinc_unit
-- asukohaga: LOINC_cleaning/data/parameter_unit_to_loinc_mapping.csv
update analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a
  set loinc_unit = p.loinc_unit
  from analysiscleaning.parameter_unit_to_loinc_unit_mapping as p
  where lower(a.parameter_unit_raw) = lower(p.parameter_unit);

-- Mitmel real LOINC kood olemas?
-- 3552907
select count(*) from analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a
where  loinc_code is not null;

------------------------------------------------------------------------------------
-- unitite consistency kontroll
------------------------------------------------------------------------------------
-- uniteid saab kahte moodi:
    -- 1. loinc_unit on saadud: PN -> PU -> loinc_unit
    -- 2. loinc_unit on saadud: PN -> LOINC code -> loinc unit
-- ideaalis peaksid 1. ja 2. loinc unitid kattuma

------------------------------------------------------------------------------------
--vaatleme ridu, kus loinc kood on olemas

-- KORRAS RIDADE ARV
-- PN->PU->loinc_unit == PN->loinc-code->loinc_unit
select count(*)
    from analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where upper(a.loinc_unit) is not distinct from upper(e.t_yhik) and
        -- loinc_no on üleüldse html-s olemas
         a.loinc_code is not null;


-- MITTE KORRAS RIDADE ARV
-- (võivad olla ka olukorrad, kus üks on null ja teine pole või vastupidi)
--  PN->PU->loinc_unit != PN->loinc-code->loinc_unit
select count(*)--distinct a.parameter_name_raw, a.loinc_unit, e.t_yhik
    from analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where upper(a.loinc_unit) is distinct from upper(e.t_yhik) and
          a.loinc_code is not null;


-- VASTUOLUS RIDADE ARV
-- loinc_unit ja t_yhik on VASTUOLUS (unit mõlemal olemas aga erinev)

select count(*)--distinct a.parameter_name_raw, a.loinc_unit, e.t_yhik
    from analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where upper(a.loinc_unit) is distinct from upper(e.t_yhik) and
          loinc_unit is not null and t_yhik is not null;
          --and a.loinc_code is not null;

-- VASTUOLUS UNITITE PAARID

/*
-- selle järgi luuakse unit_conversion_parameter_name.csv
select  a.loinc_unit, e.t_yhik, a.parameter_name_raw, count(*)
    from analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where upper(a.loinc_unit) is distinct from upper(e.t_yhik) and
          loinc_unit is not null and t_yhik is not null
group by loinc_unit, t_yhik, parameter_name_raw
order by loinc_unit, t_yhik, count desc;
*/


-- selle järgi luuakse unit_conversion.csv
select a.loinc_unit, e.t_yhik, count(*)
    from analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a
    left join classifications.elabor_analysis as e
    on a.loinc_code = e.loinc_no
    where upper(a.loinc_unit) is distinct from upper(e.t_yhik) and
          loinc_unit is not null and t_yhik is not null
group by loinc_unit, t_yhik
order by loinc_unit, t_yhik, count desc;


-- unit_conversion_parameter_name.csv
\copy (select  a.loinc_unit, e.t_yhik, a.parameter_name_raw, count(*) from analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a left join classifications.elabor_analysis as e on a.loinc_code = e.loinc_no where upper(a.loinc_unit) is distinct from upper(e.t_yhik) and loinc_unit is not null and t_yhik is not null group by loinc_unit, t_yhik, parameter_name_raw order by loinc_unit, t_yhik, count desc) to 'unit_conversion_parameter_name.csv' With (delimiter ',', format csv, header);

-- unit_conversion.csv
\copy (select a.loinc_unit, e.t_yhik, count(*) from analysiscleaning.analysis_html_loinced_runfull201903041255_unit_consistency as a left join classifications.elabor_analysis as e on a.loinc_code = e.loinc_no where upper(a.loinc_unit) is distinct from upper(e.t_yhik) and loinc_unit is not null and t_yhik is not null group by loinc_unit, t_yhik order by loinc_unit, t_yhik, count desc) to 'unit_conversion.csv' With (delimiter ',', format csv, header);

select * from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping
where parameter_name  like '%S-f12 IgE%'