/*Loon ühtse  short tabeli*/
create table analysiscleaning.short_loinc_mapping as
  (
  select * from egcut_epi.analysiscleaning.short_loinc_mapping_blood
  union
  select * from egcut_epi.analysiscleaning.short_loinc_mapping_urine
  );

/*-------------------------------------------------------------------*/
/*Loon ühtse long tabeli*/
drop table if exists analysiscleaning.long_loinc_mapping;
create table analysiscleaning.long_loinc_mapping as
  (
  select * from egcut_epi.analysiscleaning.long_loinc_mapping_blood
  union
  select * from egcut_epi.analysiscleaning.long_loinc_mapping_urine
  );

-- post corrections to achieve compatibility with loinc_official loinc classifier
-- SerPlas -> Ser/Plas
update analysiscleaning.long_loinc_mapping
set system = 'Ser/Plas'
where system = 'SerPlas';

select *
from analysiscleaning.long_loinc_mapping;


/*long_loinc_mapping has 11088 rows */
select analysis_name, parameter_name, parameter_code, unit, count(*) from analysiscleaning.long_loinc_mapping
group by analysis_name, parameter_name, parameter_code, unit;

/*-------------------------------------------------------------------*/
/*Probleem:*/

/*Andmestikus esinevad duplikaadid, mis tulenevad sellest,
  et vastav analüüs on klassifitseerimise käigus (analysis_classified) klassifitseeritud nii bloodiks kui urineks*/

/*Näide duplikaadist, kus üks rida mappitud*/
/*kood 1*/
select * from analysiscleaning.long_loinc_mapping
where analysis_name = 'Biokeemilised uuringud' and parameter_name like 'S-Gluc%' and parameter_code = '715';

/*Näide duplikaadist, kus üks mõlemad read mappimata*/
/*Kuna ei tea, kas analüüs kuulub vere või uriini alla, siis need duplikaadid jäävad sisse*/
select * from analysiscleaning.long_loinc_mapping
where analysis_name = 'Mikrobioloogilised uuringud' and parameter_name = 'Ca-Ae(Aeroobse mikrofloora külv (kateetrilt))' and parameter_code = '1740'


/*---------------------------------------------------------------------*/
/*Lahendus:*/

/*Tabel konfliktsetest võtmetest ehk (analysis_name, parameter_name, parameter_code, unit) mis tekitavad long_loinc_mappingusse konflikte*/
/*6118 rida*/
/*kood 2*/
select analysis_name, parameter_name, parameter_code, unit
      from analysiscleaning.long_loinc_mapping
      where analysis_name in
            /*nimekiri analüüsi nimedest, mis  klassifitseeritud nii vereks kui ka uriiniks, kokku 14*/
            (select analysis_name
             from analysiscleaning.analysis_classified
             group by analysis_name
             having count(analysis_name) > 1
            );

/*short tabel ehk kõik MAPPITUD konfliktsed read*/
/*kokku 286 rida*/
/*sisemine päring on kood 2*/
/*kood 3*/
select * from analysiscleaning.long_loinc_mapping
where analyte is not null and
      property is not null and
      (analysis_name, parameter_name, parameter_code, unit) in
            (select analysis_name, parameter_name, parameter_code, unit
            from analysiscleaning.long_loinc_mapping
            where analysis_name in
                /*nimekiri analüüsi nimedest, mis  klassifitseeritud nii vereks kui ka uriiniks, kokku 14*/
                (select analysis_name
                from analysiscleaning.analysis_classified
                group by analysis_name
                having count(analysis_name) > 1
                )
            );


/*Duplikaatide välja viskamine*/
/*sama kood nagu eelmine ainult tahame ridu, ainult valime read, kus property ja analyte on nullid ehk mapping puudub*/
/*võib kõik need ära kustutada, sest kui järgenvalt hakkame analysis_loinced looma, siis left joiniga tekivad vajalikud tagasi*/
/*kõige sisemine pärin kood 2*/
/*kood 4*/
delete from analysiscleaning.long_loinc_mapping
where analyte is null and
      property is null and
      (analysis_name, parameter_name, parameter_code, unit) in
            (select analysis_name, parameter_name, parameter_code, unit
            from analysiscleaning.long_loinc_mapping
            where analysis_name in
                /*nimekiri analüüsi nimedest, mis  klassifitseeritud nii vereks kui ka uriiniks, kokku 14*/
                (select analysis_name
                from analysiscleaning.analysis_classified
                group by analysis_name
                having count(analysis_name) > 1
                )
            );


/*kontroll näite põhjal (kood 1), et enam long_loinc_mappingus ei esine duplikaadina*/
select * from analysiscleaning.long_loinc_mapping
where analysis_name = 'Biokeemilised uuringud' and parameter_name like 'S-Gluc%' and parameter_code = '715';


/*Duplikaat, kus nii blood kui ka urine oli mappimata, jäävad duplikaatidena sisse*/
select * from analysiscleaning.long_loinc_mapping
where analysis_name = 'Mikrobioloogilised uuringud' and parameter_name = 'Ca-Ae(Aeroobse mikrofloora külv (kateetrilt))' and parameter_code = '1740'


/*uues duplikaadivabas lond_loinc_mappingus on 10523 rida */
/*algses long_loinc_mappingus oli 11088 rida, parandasime tulemust 565 rea ehk 5% võrra*/
select analysis_name, parameter_name, parameter_code, unit, count(*) from analysiscleaning.long_loinc_mapping
group by analysis_name, parameter_name, parameter_code, unit;

