-- erinevaid AN ja PC paare 3265
select distinct analysis_name, parameter_code_raw from work.run_ac_html_201901171409_analysis_entry

-- pöörame t_nimetus to AN mappingu ümber ja saame:
-- AN to t_nimetus/ t_lyhend mapping
create table analysiscleaning.analysis_name_to_t_nimetus_mapping as
select distinct t_nim.analysis_name, el.t_nimetus, el.t_lyhend  from classifications.elabor_analysis as el
          right join analysiscleaning.t_nimetus_to_analysis_name_mapping as t_nim
          on t_nim.t_nimetus = el.t_nimetus
          where t_nim.analysis_name is not null


-- PN -> PC mapping
-- 1604 rida
create table analysiscleaning.parameter_name_to_parameter_code_mapping as
    select distinct parameter_name, parameter_code_raw from work.run_ac_html_201901171409_analysis_entry
    where parameter_name is not null and parameter_code_raw is not null


-- tahame: mapime entryle kõigepealt AN järgi t_lyhendi
select e.id, e.analysis_name, e.parameter_code_raw, an.t_lyhend
from work.run_ac_html_201901171409_analysis_entry as e
    left join analysiscleaning.analysis_name_to_t_nimetus_mapping as an
    on an.analysis_name = e.analysis_name
where an.t_lyhend is not null;

-- tahame: mapime PC järgi t_lyhendi
select e.id, e.analysis_name, e.parameter_code_raw, pc."loinc_code(T-luhend)"
from work.run_ac_html_201901171409_analysis_entry as e
    left join analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as pc
    on pc.parameter_code_raw = e.parameter_code_raw
where "loinc_code(T-luhend)" is not null

-- erinevaid t_lyhendeid 4676 (kõik unikaalsed)
select distinct * from classifications.elabor_analysis;
--erinevaid t_nimetusi 4626
select distinct t_nimetus from classifications.elabor_analysis;
--erinevaid kasutatav_nimetusi 4350
select distinct kasutatav_nimetus from classifications.elabor_analysis;



/*
-- võrdluseks ühendame need kaks

-- mapime entryle kõigepealt AN järgi t_lyhendi ja sis PC järgi t_lyhendi
        (select e.id, an.analysis_name, e.parameter_code_raw, an.t_lyhend
          from work.run_ac_html_201901171409_analysis_entry as e
          left join analysiscleaning.analysis_name_to_t_nimetus_mapping as an
          on an.analysis_name = e.analysis_name
        --where t_lyhend is not null
        order by id
        )
*/
--mapime PC järgi t_lyhendi KOGU entryle
drop table if exists analysiscleaning.t_lyhend_to_entry_mapping_whole_table;
create table analysiscleaning.t_lyhend_to_entry_mapping_whole_table as
         (
         select pc."loinc_code(T-luhend)", e.* --e.id, e.analysis_name, e.parameter_code_raw,
         from work.run_ac_html_201901171409_analysis_entry as e
              left join analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as pc
              on lower(pc.parameter_code_raw) = lower(e.parameter_code_raw)
          );
-- t_lyhendi olemas 23628 real

--kuna PC == PN mõnikord, siis mapime eelenvalt mappimata jäänutele PN abil t_lyhendi
update analysiscleaning.t_lyhend_to_entry_mapping_whole_table as t
set "loinc_code(T-luhend)" = pc."loinc_code(T-luhend)"
        from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as pc
        where lower(pc.parameter_code_raw) = lower(t.parameter_name) and t."loinc_code(T-luhend)" is null;
-- t_lyhend olemas 24442 real

-- Võib ka juhtuda et AN == PC nt AN == MCHC, peaks ka seal rakendama PC mappingut
update analysiscleaning.t_lyhend_to_entry_mapping_whole_table as t
set "loinc_code(T-luhend)" = pc."loinc_code(T-luhend)"
        from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as pc
        where lower(pc.parameter_code_raw) = lower(t.analysis_name) and t."loinc_code(T-luhend)" is null;
-- t_lyhend olemas 31331 real

-- t_lyhend olemas 31331 / 71845 = 43 %

--nüüd vaatame, kas AN, PC abil näeme käsitsi mingeid seosed
select distinct analysis_name, parameter_name, parameter_code_raw from analysiscleaning.t_lyhend_to_entry_mapping_whole_table
where "loinc_code(T-luhend)" is null;

-- kui palju parandaks, kui kasutatda dirty_code_to_t_lyhed_mapping_all

select * from analysiscleaning.parameter_name_to_parameter_code_mapping
where parameter_code_raw like 'Alb%';

select * from work.run_ac_html_201901171409_analysis_entry
where parameter_code_raw like 'Alb%';



select *
from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where dirty_code like '%UBG%';

select *
from work.run_ac_html_201901171409_analysis_entry
where analysis_name = 'MCHC';