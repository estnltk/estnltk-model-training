------------------------------------------------------------------------------------------------------------

-- PARAMETER NAME -> T_LYHEND MAPPING

------------------------------------------------------------------------------------------------------------
-- erinevaid PN kokku 1426
select distinct parameter_name from work.run_ac_html_201901171409_analysis_entry;
-- erinevaid PN kokku, liiga palju andmeid, 18697
select distinct parameter_name_raw from work.runfull201903041255_analysis_html;

select * from work.runfull201903041255_analysis_entry
  limit 1


-- rakendame kõiki csv failides olevaid cda-data-cleaning/analysis_data_cleaning/name_cleaning/data reegleid
-- kõik reeglid tabelis dirty_code_to_t_lyhend_mapping_all
drop table if exists analysiscleaning.parameter_name_to_t_lyhend_mapping;
create table analysiscleaning.parameter_name_to_t_lyhend_mapping as
select distinct e.parameter_name_raw, a."loinc_code(T-luhend)", count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as e
left join analysiscleaning.dirty_code_to_t_lyhed_mapping_all as a
    on lower(e.parameter_name_raw) = lower(a.dirty_code)
group by parameter_name_raw, a."loinc_code(T-luhend)"
order by count;

-- mappimata jäänud read 17 115
select distinct * from analysiscleaning.parameter_name_to_t_lyhend_mapping
where "loinc_code(T-luhend)" is null;

-- puhastame mappimata jäänutel parameter_name_raw jupyteri notebooki parameter_code_normalization.ipynb abil
--select distinct parameter_name_raw, "loinc_code(T-luhend)" from analysiscleaning.parameter_name_to_t_lyhend_mapping
--where "loinc_code(T-luhend)" is null;

drop table if exists analysiscleaning.dirty_parameter_name_cleaned
-- loome tabeli puhastatud PN jaoks
create table analysiscleaning.dirty_parameter_name_cleaned (
  dirty_code varchar,
  clean_code varchar
);


--------------------------------------------------------------
--puhastamine

-- notebooki abil täidame tabeli
-- lisame PN to t_lyhend mappingu tabelisse puhastatud nimed
ALTER TABLE analysiscleaning.parameter_name_to_t_lyhend_mapping
  ADD parameter_name_clean varchar;

update analysiscleaning.parameter_name_to_t_lyhend_mapping as p
set parameter_name_clean = c.clean_code
from analysiscleaning.dirty_parameter_name_cleaned as c
where lower(p.parameter_name_raw) = lower(c.dirty_code);

--kui loinc sai määratud juba tavalise PN järgi, siis see ongi juba puhas
update analysiscleaning.parameter_name_to_t_lyhend_mapping as p
set parameter_name_clean = parameter_name_raw
where "loinc_code(T-luhend)" is not null;
-----------------------------------------------------------------
-- mappime puhastatud PN_clean järgi
update analysiscleaning.parameter_name_to_t_lyhend_mapping as p
set "loinc_code(T-luhend)" = a."loinc_code(T-luhend)"
    from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as a
    where lower(p.parameter_name_clean) = lower(a.dirty_code) and
          p."loinc_code(T-luhend)" is null;

-- nüüd mapitud 303 rida 1426st
-----------------------------------------------------------------
-- mapime e-labori järgi
update analysiscleaning.parameter_name_to_t_lyhend_mapping as p
set "loinc_code(T-luhend)" = e.t_lyhend
from classifications.elabor_analysis as e
    where p."loinc_code(T-luhend)" is null and
          (lower(p.parameter_name_raw) = lower(e.t_lyhend));

update analysiscleaning.parameter_name_to_t_lyhend_mapping as p
set "loinc_code(T-luhend)" = e.t_lyhend
from classifications.elabor_analysis as e
    where p."loinc_code(T-luhend)" is null and
          (lower(p.parameter_name_clean) = lower(e.t_lyhend));
-- nüüd mapitud 340 rida 1426st

select distinct *
from analysiscleaning.parameter_name_to_t_lyhend_mapping
where "loinc_code(T-luhend)" is null;
--16039 15989










-- katame ära 5559/10494 = 52% entry mitte tühjadest PN-st
select sum(count) from analysiscleaning.parameter_name_to_t_lyhend_mapping
where parameter_name_raw is not null; ---and "loinc_code(T-luhend)" is not null;


-- MÄÄRA KÄSITSI
-- käsitsi võime määrata ära mingid null
-- suht ilmselged


select *
from analysiscleaning.parameter_name_to_t_lyhend_mapping
where "loinc_code(T-luhend)" is null and parameter_name is not null;

