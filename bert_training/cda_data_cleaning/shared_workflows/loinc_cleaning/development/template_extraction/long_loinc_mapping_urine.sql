/*URIIN*/
alter table egcut_epi.analysiscleaning.long_loinc_mapping_urine owner to egcut_epi_analysiscleaning_create;
grant select on egcut_epi.analysiscleaning.long_loinc_mapping_urine to egcut_epi_analysiscleaning_read;

alter table egcut_epi.analysiscleaning.short_loinc_mapping_urine owner to egcut_epi_analysiscleaning_create;
grant select on egcut_epi.analysiscleaning.short_loinc_mapping_urine to egcut_epi_analysiscleaning_read;

alter table egcut_epi.analysiscleaning.pre_long_mapping_urine owner to egcut_epi_analysiscleaning_create;
grant select on egcut_epi.analysiscleaning.pre_long_mapping_urine to egcut_epi_analysiscleaning_read;

/*PRE LONG*/
/*4300*/
drop table if exists analysiscleaning.pre_long_mapping_urine;
create table analysiscleaning.pre_long_mapping_urine as
  select analysis_name, parameter_name, parameter_code, unit, analyte, property, system_type
  from egcut_epi.analysiscleaning.mapped_urine_templates
  /*where analysis_name = 'Hematoloogilised uuringud'*/
  group by analysis_name, parameter_name, parameter_code, unit, analyte, property, system_type;

----------------------------------------------------------------------------------------------------
/*SHORT*/
/*Jätame välja uniti, saame mappingu, analysis_name, parameter_name, parameter_code järgi*/
/*380*/
drop table if exists analysiscleaning.short_loinc_mapping_urine;
create table analysiscleaning.short_loinc_mapping_urine as
select distinct analysis_name, parameter_name, parameter_code, analyte, property, system_type
from egcut_epi.analysiscleaning.mapped_urine_templates
where /*analysis_name = 'Hematoloogilised uuringud' and*/
      analyte is not null and
      system_type is not null and
      property is not null and
      property != 'unknown'
group by analysis_name, parameter_name, parameter_code, unit, analyte, property, system_type;

--------------------------------------------------------------------------------------------------

/*LONG - parandab konflikte*/
/*Mapib seni uniti tõttu mappimata  jäänud read*/
/*Mapib ära SHORTi põhjal PRE-LONG tabelis kohad kus (a, p, s) puudu,
sõltumata unitist mapping */
drop table if exists analysiscleaning.long_loinc_mapping_urine;
select * into analysiscleaning.long_loinc_mapping_urine from analysiscleaning.pre_long_mapping_urine;

update egcut_epi.analysiscleaning.long_loinc_mapping_urine as long
set analyte = short.analyte,
    property = short.property,
    system_type = short.system_type
from egcut_epi.analysiscleaning.short_loinc_mapping_urine as short
where /*(long.unit is null or long.unit = '-' or long.unit = '')*/
    short.analysis_name = long.analysis_name and
    short.parameter_name = long.parameter_name and
    (short.parameter_code = long.parameter_code or
    /*kui mõlemas tabelis parameter_code on null, siis null = null on null ja MITTE true ehk vaja nullide jaoks eraldi tingimust*/
        (
        long.parameter_code is null and
        short.parameter_code is null
        )
    );

--------------------------------------------------------------------------------------------------------

/*Prelongis 404 ilma null mappinguta rida;
 shortis 377
 longis 405 */
select * from analysiscleaning.pre_long_mapping_urine
where property is not null and property !='unknown' and analyte is not null;
--group by  analysis_name, parameter_name, parameter_code, unit, analyte, property, system_type;

select * from analysiscleaning.short_loinc_mapping_urine
where property is not null and property !='unknown' and analyte is not null;

select * from analysiscleaning.long_loinc_mapping_urine
where property is not null and property !='unknown' and analyte is not null;



-------------------------------------------------------------------------------
/*EI TEE LISAPARANDUSI VÕRRELDES EELNEVAGA*/
/*2**3 = 8 inserti
Pole olukorda, kus system oleks null, jääb alles 4 inserti */
/*Analyte, property, system*/
/* jah ei jah*/
update egcut_epi.analysiscleaning.long_loinc_mapping_urine as long
set property = short.property
from egcut_epi.analysiscleaning.short_loinc_mapping_urine as short
where long.property is null and
      short.analysis_name = long.analysis_name and
      short.parameter_name = long.parameter_name and
          (
          short.parameter_code = long.parameter_code or
              /*kui mõlemas tabelis parameter_code on null, siis null = null on null ja MITTE true ehk vaja nullide jaoks eraldi tingimust*/
              (
              long.parameter_code is null and
              short.parameter_code is null
              )
          );

/*Analyte, property, system*/
/* ei jah jah*/
update egcut_epi.analysiscleaning.long_loinc_mapping_urine as long
set analyte = short.analyte
from egcut_epi.analysiscleaning.short_loinc_mapping_urine as short
where long.analyte is null and
    short.analysis_name = long.analysis_name and
    short.parameter_name = long.parameter_name and
          (
          short.parameter_code = long.parameter_code or
              /*kui mõlemas tabelis parameter_code on null, siis null = null on null ja MITTE true ehk vaja nullide jaoks eraldi tingimust*/
              (
              long.parameter_code is null and
              short.parameter_code is null
              )
          );

/*Analyte, property, system*/
/* ei ei jah*/
update egcut_epi.analysiscleaning.long_loinc_mapping_urine as long
set analyte = short.analyte,
    property = short.property
from egcut_epi.analysiscleaning.short_loinc_mapping_urine as short
where long.analyte is null and
    long.property is null and
    short.analysis_name = long.analysis_name and
    short.parameter_name = long.parameter_name and
          (
          short.parameter_code = long.parameter_code or
              /*kui mõlemas tabelis parameter_code on null, siis null = null on null ja MITTE true ehk vaja nullide jaoks eraldi tingimust*/
              (
              long.parameter_code is null and
              short.parameter_code is null
              )
          );


-------------------------------------------------------------------------------------
--Muutused

/*KONTROLL, et parandab
Pre_long:
analysis_name,parameter_name,parameter_code,unit,analyte,property,system_type
Uriini analüüs 10-parameetrilise testribaga,Glükoos,U-Strip,mmol/l,Gluc,SCnc,urine
Uriini analüüs 10-parameetrilise testribaga,Glükoos,U-Strip,-,     Gluc,unknown,  urine

Long_mapping
analysis_name,parameter_name,parameter_code,unit,analyte,property,system_type
Uriini analüüs 10-parameetrilise testribaga,Glükoos,U-Strip,-,     Gluc,SCnc,urine
Uriini analüüs 10-parameetrilise testribaga,Glükoos,U-Strip,mmol/l,Gluc,SCnc,urine
*/


/*Kui palju erinevate analyte, property, system kombinatsioonidega paneele on algselt (pre_loincing) - ÜKS*/
/*long_loincing - 0*/
select analysis_name, parameter_code, parameter_name, array_agg(distinct (analyte, property, system_type))
from egcut_epi.analysiscleaning.pre_long_mapping_urine
group by analysis_name, parameter_code, parameter_name
having array_length(array_agg(distinct (analyte, property, system_type)), 1) > 1;



/*PARANDATUD READ =  1*/
select distinct p.analysis_name, l.analysis_name, p.analyte, l.analyte, p.property, l.property
from analysiscleaning.pre_long_mapping_urine as p, analysiscleaning.long_loinc_mapping_urine as l
where (p.property != l.property or p.analyte != l.analyte) and
    p.analysis_name = l.analysis_name and
    p.parameter_name = l.parameter_name and
    (p.parameter_code = l.parameter_code or
    /*kui mõlemas tabelis parameter_code on null, siis null = null on null ja MITTE true ehk vaja nullide jaoks eraldi tingimust*/
        (
        l.parameter_code is null and
        p.parameter_code is null
        )
    );


/*14 analysis_name'i on klasifitseeritud nii bloodiks kui ka uriiniks?*/
select * from (
              select analysis_name, array_agg(system_type) as types
              from egcut_epi.analysiscleaning.analysis_classified
              group by analysis_name
              ) as table1
where array_length(types, 1) > 1















/*KATSETUS*/
/*Preliminary BLOOD_mapping*/
/*(AN, PN, PC) vastav mapping, koos unitiga*/
/*Kokku  9 200 rida*/
/*6 500 real on nii analyte kui ka property null/unknown*/
/*2 600 mappimata rea korral on analysis_name system mitte ühene ehk nii urine kui ka blood*/
select analysis_name, upper(parameter_name) as parameter_name, parameter_code, unit, system, analyte, property
from egcut_epi.analysiscleaning.mapped_blood_templates
where (analysis_name, upper(parameter_name), parameter_code) in
    (
    /*kõik unikaalsed (AN, PN, PC) kombinatsioonid*/
    select  analysis_name, upper(parameter_name), parameter_code
    from egcut_epi.analysiscleaning.mapped_blood_templates
    group by analysis_name, upper(parameter_name), parameter_code
    ) and unit is null and analyte is not null
group by analysis_name, upper(parameter_name), parameter_code, unit, system, analyte, property;


/* Preliminary_URINE_mapping*/
/* (AN, PN, PC) vastav mapping, koos unitiga*/
/* Kokku 4 200 rida*/
/* 3 800 real on nii analyte kui ka property null*/
/* Kui välja jätta biokeemia analüüs, siis 40 000 real on nii analyte kui ka property null*/
/* 2 900  mappimata rea korral on analysis_name'i system_type mitte ühene ehk siis nii blood kui ka urine*/
select analysis_name, upper(parameter_name) as parameter_name, parameter_code, unit, system_type as system, analyte, property
from egcut_epi.analysiscleaning.mapped_urine_templates
where (analysis_name, upper(parameter_name), parameter_code) in
    (
    /*kõik unikaalsed (AN, PN, PC) kombinatsioonid*/
    select  analysis_name, upper(parameter_name), parameter_code
    from egcut_epi.analysiscleaning.mapped_urine_templates
    group by analysis_name, upper(parameter_name), parameter_code
    )
group by analysis_name, upper(parameter_name), parameter_code, unit, system_type, analyte, property;


/*Sellised uriinid jätab mappimata*/
select distinct analysis_name from egcut_epi.analysiscleaning.mapped_urine_templates
where analyte is null and property is null;


/*---------------------------------------*/
/*AINULT kasulikku infot sisaldavad read:*/

/*URINE*/
/*Jätame long_loinc_mappingusse alles vaid need read, kus analyte on not null ja property on not null ja not unknown*/
/*380 rida*/

select analysis_name, upper(parameter_name) as parameter_name, parameter_code, unit, system_type as system, analyte, property
from egcut_epi.analysiscleaning.mapped_urine_templates
where (analysis_name, upper(parameter_name), parameter_code) in
    (
    /*kõik unikaalsed (AN, PN, PC) kombinatsioonid*/
    select  analysis_name, upper(parameter_name), parameter_code
    from egcut_epi.analysiscleaning.mapped_urine_templates
    group by analysis_name, upper(parameter_name), parameter_code
    )
    and analyte is not null and property is not null and property != 'unknown'
group by analysis_name, upper(parameter_name), parameter_code, unit, system_type, analyte, property;


/*BLOOD*/
/*2 400 rida*/
select analysis_name, upper(parameter_name) as parameter_name, parameter_code, unit, system_type as system, analyte, property
from egcut_epi.analysiscleaning.mapped_blood_templates
where  (analysis_name, upper(parameter_name), parameter_code) in
    (
    /*kõik unikaalsed (AN, PN, PC) kombinatsioonid*/
    select  analysis_name, upper(parameter_name), parameter_code
    from egcut_epi.analysiscleaning.mapped_blood_templates
    group by analysis_name, upper(parameter_name), parameter_code
    )
    and analyte is not null and property is not null and property != 'unknown'
group by analysis_name, upper(parameter_name), parameter_code, unit, system_type, analyte, property;







