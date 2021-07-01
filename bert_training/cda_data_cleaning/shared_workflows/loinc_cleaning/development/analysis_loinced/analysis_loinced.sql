drop table if exists analysiscleaning.analysis_loinced;

create table egcut_epi.analysiscleaning.analysis_loinced as
  select
       epi_id,
       long.analyte,
       long.property,
       long.system,
       long.time_aspect,
       long.scale,
       long.unit,
       matched.analysis_name,
       matched.parameter_name,
       matched.parameter_code_raw_entry as parameter_code,
      matched.code_system_name as code_system_type,
      /*code_system juurde, tto_oid nimeks*/
      matched.effective_time,
      matched.value,
      matched.reference_values,
      (substring(code_system, '[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.')) as tto_oid
  from egcut_epi.analysiscleaning.r07_analysis_matched as matched
  left join egcut_epi.analysiscleaning.long_loinc_mapping as long
    on long.analysis_name = matched.analysis_name and
       long.parameter_name = matched.parameter_name and
       long.parameter_code = matched.parameter_code_raw_entry;

alter table egcut_epi.analysiscleaning.analysis_loinced owner to egcut_epi_analysiscleaning_create;
grant select on egcut_epi.analysiscleaning.analysis_loinced to egcut_epi_analysiscleaning_read;

create index analysis_loinced_epi_id_idx on analysiscleaning.analysis_loinced(epi_id);

create index analysis_loinced_loinc_idx on analysiscleaning.analysis_loinced(analyte, property, system, time_aspect, scale, unit);

create index analysis_loinced_effective_time_idx on analysiscleaning.analysis_loinced(effective_time);

/*Kokku ridu 2 161 161*/
/*Parandatud long_loinc_mappingu korral ridu 2 071 020*/
select count(*) from analysiscleaning.analysis_loinced;

/*Mapitud  ridu 1 234 744
Mapitud seega 57% andmetest*/
select count(*) from analysiscleaning.analysis_loinced
where analyte is not null and property is not null and property != 'unknown';


select * from analysiscleaning.analysis_loinced
where  analysis_name='Biokeemilised uuringud' and  parameter_name='S-Gluc(Glükoos seerumis)' and  parameter_code='715'

