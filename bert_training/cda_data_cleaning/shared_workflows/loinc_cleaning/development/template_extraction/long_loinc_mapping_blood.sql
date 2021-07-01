/*BLOOD MAPPING*/
alter table egcut_epi.analysiscleaning.long_loinc_mapping_blood owner to egcut_epi_analysiscleaning_create;
grant select on egcut_epi.analysiscleaning.long_loinc_mapping_blood to egcut_epi_analysiscleaning_read;

alter table egcut_epi.analysiscleaning.short_loinc_mapping_blood owner to egcut_epi_analysiscleaning_create;
grant select on egcut_epi.analysiscleaning.short_loinc_mapping_blood to egcut_epi_analysiscleaning_read;

alter table egcut_epi.analysiscleaning.pre_long_mapping_blood owner to egcut_epi_analysiscleaning_create;
grant select on egcut_epi.analysiscleaning.pre_long_mapping_blood to egcut_epi_analysiscleaning_read;

-------------------------------------------------------------------------------------------
/*PRE LONG*/
/*Olemas ka uniti read, mitmetel kordadel on unit - või null*/
/*9750*/
drop table if exists analysiscleaning.pre_long_mapping_blood;
create table analysiscleaning.pre_long_mapping_blood as
  select analysis_name, parameter_name, parameter_code, unit, analyte, property, system
  from egcut_epi.analysiscleaning.mapped_blood_templates
  /*where analysis_name = 'Hematoloogilised uuringud'*/
  group by analysis_name, parameter_name, parameter_code, unit, analyte, property, system;

-------------------------------------------------------------------------------------------
/*SHORT*/
/*2500*/
/*Jätame välja uniti, saame mappingu, analysis_name, parameter_name, parameter_code järgi*/
drop table if exists analysiscleaning.short_loinc_mapping_blood;
create table analysiscleaning.short_loinc_mapping_blood as
select distinct analysis_name, parameter_name, parameter_code, analyte, property, system
from egcut_epi.analysiscleaning.mapped_blood_templates
where /*analysis_name = 'Hematoloogilised uuringud' and*/
      analyte is not null and
      system is not null and
      property is not null and
      property != 'unknown'
group by analysis_name, parameter_name, parameter_code, unit, analyte, property, system;


-------------------------------------------------------------------------------------------
/*LONG*/
/*Mapib seni uniti tõttu mappimata  jäänud read*/
/*Mapib ära SHORTi põhjal PRE-LONG tabelis kohad kus (a, p, s) puudu,
sõltumata unitist mapping */
drop table if exists analysiscleaning.long_loinc_mapping_blood;
select * into analysiscleaning.long_loinc_mapping_blood from analysiscleaning.pre_long_mapping_blood;

update egcut_epi.analysiscleaning.long_loinc_mapping_blood as long
set analyte = short.analyte,
    property = short.property,
    system = short.system
from egcut_epi.analysiscleaning.short_loinc_mapping_blood as short
where
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


/*Prelongis 2609 ilma null mappinguta rida;
shortis 2550
longis 2625 */
select * from analysiscleaning.pre_long_mapping_blood
where property is not null and property !='unknown' and analyte is not null
--group by  analysis_name, parameter_name, parameter_code, unit, analyte, property, system;

select * from analysiscleaning.long_loinc_mapping_blood
where property is not null and property !='unknown' and analyte is not null;



-------------------------------------------------------------------------------------------
/*EI TEE LISAPARANDUSI VÕRRELDES EELNEVAGA*/
/*2**3 = 8 inserti
Pole olukorda, kus system oleks null, jääb alles 4 inserti */
/*Analyte, property, system*/
/* jah ei jah*/
update egcut_epi.analysiscleaning.long_loinc_mapping_blood as long
set property = short.property
from egcut_epi.analysiscleaning.short_loinc_mapping_blood as short
where long.property is null and
      short.analysis_name = long.analysis_name and
      short.parameter_name = long.parameter_name and
      short.parameter_name = long.parameter_name and
          (
          short.parameter_code = long.parameter_code or
              (
              long.parameter_code is null and
              short.parameter_code is null
              )
          );
/*Analyte, property, system*/
/* ei jah jah*/
update egcut_epi.analysiscleaning.long_loinc_mapping_blood as long
set analyte = short.analyte
from egcut_epi.analysiscleaning.short_loinc_mapping_blood as short
where long.analyte is null and
    short.analysis_name = long.analysis_name and
    short.parameter_name = long.parameter_name and
          (
          short.parameter_code = long.parameter_code or
              (
              long.parameter_code is null and
              short.parameter_code is null
              )
          );
/*Analyte, property, system*/
/* ei ei jah*/
update egcut_epi.analysiscleaning.long_loinc_mapping_blood as long
set analyte = short.analyte,
    property = short.property
from egcut_epi.analysiscleaning.short_loinc_mapping_blood as short
where long.analyte is null and
    long.property is null and
    short.analysis_name = long.analysis_name and
    short.parameter_name = long.parameter_name and (
          short.parameter_code = long.parameter_code or
          (long.parameter_code is null and
           short.parameter_code is null)
          );

select * from egcut_epi.analysiscleaning.long_loinc_mapping_blood
where analyte is null and parameter_code ~ '^[0-9]*$'

---group by parameter_code ~ '^[0-9]*$';

select analysis_name, count(*) from egcut_epi.analysiscleaning.long_loinc_mapping_blood
where analyte is null and parameter_code ~ '^[0-9]*$'
group by analysis_name

select parameter_code, parameter_name, count(*) from egcut_epi.analysiscleaning.long_loinc_mapping_blood
where analyte is null and parameter_code ~ '^[0-9]*$' and analysis_name = 'Hematoloogilised uuringud'
group by parameter_code, parameter_name;



------------------------------------------------------------------------------------------------------------------------
--muutuste ja andmestiku uurimine

/*Kui palju erinevate unititega paneele*/
select analysis_name, parameter_code, parameter_name, array_agg(unit) from egcut_epi.analysiscleaning.long_loinc_mapping_blood
group by analysis_name, parameter_code, parameter_name
having array_length(array_agg(unit), 1) > 1;

select analysis_name, parameter_code, parameter_name, array_agg(distinct (analyte, property, system)), count(*)
from analysiscleaning.pre_long_mapping_blood
group by analysis_name, parameter_code, parameter_name
order by count desc;


/*Kui palju erinevate analyte, property, system kombinatsioonidega paneele on algselt (pre_loincing)*/
/*Pole enam ühtegi long_loincing*/
select analysis_name, parameter_code, parameter_name, array_agg(distinct (analyte, property, system))
from egcut_epi.analysiscleaning.pre_long_mapping_blood
group by analysis_name, parameter_code, parameter_name
having array_length(array_agg(distinct (analyte, property, system)), 1) > 1;


/*Muutuste kontroll*/
select property, count(*) from analysiscleaning.pre_long_mapping_blood
where property = 'unknown' or property is null or property = ''
group by property;

select property, count(*) from analysiscleaning.long_loinc_mapping_blood
where  property = 'unknown' or property is null or property = ''
group by property;


/*PARANDATUD READ = 17*/
select distinct p.analysis_name, l.analysis_name, p.analyte, l.analyte, p.property, l.property
from analysiscleaning.pre_long_mapping_blood as p, analysiscleaning.long_loinc_mapping_blood as l
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

/*Tõestus, et erinevused pre_longi ja longi vahel on*/
/*Enne*/
select * from analysiscleaning.pre_long_mapping_blood
where analysis_name = 'Vereäige mikroskoopia' and
      parameter_name = 'Eosinofiilid' and
      parameter_code = 'B-Smear-m';

/*Pärast*/
select * from analysiscleaning.long_loinc_mapping_blood
where analysis_name = 'Vereäige mikroskoopia' and
      parameter_name = 'Eosinofiilid' and
      parameter_code = 'B-Smear-m';




/*kontrolliks*/
/*hea lühike näide, kuidas konflikte parandab 'Hemogramm, lihtne (mikroskoopia)' */

/*analysis_name,parameter_name,parameter_code,unit,analyte,property,system
Vereäige mikroskoopia,Eosinofiilid,B-Smear-m, - ,Eo,unknown,Bld
Vereäige mikroskoopia,Eosinofiilid,B-Smear-m, % ,Eo,NCnc,Bld*/

/*analysis_name,parameter_name,parameter_code,unit,analyte,property
Vereäige mikroskoopia,Lümfotsüüdid,B-Smear-m,%,Lymph,NCnc
Vereäige mikroskoopia,Lümfotsüüdid,B-Smear-m,-,Lymph,unknown

/*
analysis_name,parameter_name,parameter_code,unit,analyte,property
Vereäige mikroskoopia,Polükromaasia,B-Smear-m,,Baso,NCnc
Vereäige mikroskoopia,Polükromaasia,B-Smear-m,-,,
*/

*/

/*HEMA
analysis_name,parameter_name,parameter_code,unit,analyte,property,system
Hematoloogilised uuringud,RBC(Erütrotsüüdid veres),606,10 U/l,RBC,unknown,Bld
Hematoloogilised uuringud,RBC(Erütrotsüüdid veres),606,/pL,   RBC,NCnc,Bld

analysis_name,parameter_name,parameter_code,unit,analyte,property
Hematoloogilised uuringud,WBC(Leukotsüüdid täisverest),605,10 U/l,WBC,unknown
Hematoloogilised uuringud,WBC(Leukotsüüdid täisverest),605,/nL   ,WBC,NCnc
*/


/*
analysis_name,parameter_name,parameter_code,unit,analyte,property
B-autom5diff,MONO,,10*9/L,Mono,unknown
B-autom5diff,MONO,,%,     Mono,NCnc

analysis_name,parameter_name,parameter_code,unit,analyte,property
B-autom5diff,LYM,,10*9/L,Lymph,unknown
B-autom5diff,LYM,,%,     Lymph,NCnc
*/



select * from analysiscleaning.pre_long_mapping_blood
where parameter_name like '%lümf%'


----------------------------------------------------------------------------------------------------
/*Ülevaade mappimata  ridaest*/

select count(*) from analysiscleaning.pre_long_mapping_blood; --9750
select count(*) from analysiscleaning.long_loinc_mapping_blood; --9750
select count(*) from analysiscleaning.pre_long_mapping_urine; --4381
select count(*) from analysiscleaning.long_loinc_mapping_urine; --4381

--7141
select count(*) from analysiscleaning.pre_long_mapping_blood
where  property = 'unknown' or property is null or property = '' or analyte is null;

--7125
select count(*) from analysiscleaning.long_loinc_mapping_blood
where  property = 'unknown' or property is null or property = ''or analyte is null;

--3977
select count(*) from analysiscleaning.pre_long_mapping_urine
where  property = 'unknown' or property is null or property = '' or analyte is null;

--3976
select count(*) from analysiscleaning.long_loinc_mapping_urine
where  property = 'unknown' or property is null or property = '' or analyte is null;


