--entrys kokku ridu 66814
select count(*) from work.run_ac_html_201901171409_analysis_entry;

--erinevaid parameter_name on 1425
select distinct parameter_name, count(*) from work.run_ac_html_201901171409_analysis_entry
group by parameter_name
order by count desc ;

--erinevaid parameter_code on 2058
select distinct parameter_code_raw, count(*) from work.run_ac_html_201901171409_analysis_entry
group by parameter_code_raw;

--erinevaid parameter_name ja parameter_code kombinatsioone 2425
select distinct parameter_name, parameter_code_raw, count(*) from work.run_ac_html_201901171409_analysis_entry
group by parameter_name, parameter_code_raw;

--erinevaid parameter_name ja parameter_code kombinatsioone 1687, KUS PN EI OLE NULL
select distinct parameter_name, parameter_code_raw, count(*) from work.run_ac_html_201901171409_analysis_entry
where parameter_name is not null
group by parameter_name, parameter_code_raw;


--------------------------------------------------------------------------------------------------------------
-- panen kokku kõik Lisa tehtud csv reeglite failid

drop table if exists analysiscleaning.dirty_code_to_t_lyhed_mapping_all;
create table analysiscleaning.dirty_code_to_t_lyhed_mapping_all (
  dirty_code varchar,
  "loinc_code(T-luhend)" varchar
);

insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
select * from analysiscleaning.latest_name_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
select * from analysiscleaning.latest_parentheses_one_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
select * from analysiscleaning.latest_parentheses_two_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
select * from analysiscleaning.latest_parentheses_three_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
select * from analysiscleaning.latest_semimanual_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, "loinc_code(T-luhend)")
select * from analysiscleaning.latest_prefix_cases;
--seda peaks rakendama arvestades uniteid
--insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all (dirty_code, unit, "loinc_code(T-luhend)", loinc_unit)
--select * from analysiscleaning.latest_subspecial_cases;
insert into analysiscleaning.dirty_code_to_t_lyhed_mapping_all(dirty_code, "loinc_code(T-luhend)")
select * from analysiscleaning.latest_manual_cases;


------------------------------------------------------------------------------------------------------------------

-- PARAMETER_CODE

----------------------------------------------------------------------------------------------------------------
-- vaja oleks cleaninda parameter_name'd ja siis teha mapping
-- osa PC on juba varasemate reeglite põhjal mapitavavd
-- jagame distinct PC kahte tabelisse
-- 1. kus kõigi vanade reeglite poolt mapitud t_lyhend
-- 2. kus PC ei oska t_lyhendit määrata, aga seal cleanime/klasterdame PC ära ja siis lisame t_lyhendi



--kooku  erinevaid mittenumbrilis PC on 1189
select distinct parameter_code_raw, count(*)
from work.run_ac_html_201901171409_analysis_entry
where not (parameter_code_raw  ~ '^[0-9]*$')
group by parameter_code_raw;

-- juba ära mappitud, need on triviaalsed ja ei pea enam puhastama
--entryst on mapitud 157 PC, need ei paku huvi
select distinct e.parameter_code_raw, a."loinc_code(T-luhend)"  from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as a
right join work.run_ac_html_201901171409_analysis_entry as e
    on lower(a.dirty_code) = lower(e.parameter_code_raw)
where dirty_code is not null and
    not (parameter_code_raw  ~ '^[0-9]*$');

--ülejäänud 1189 -157 = 1032 pakuvad huvi

-- järgnevad koodid entryst on mappimata, neid vaja jupyter notebookis puhastada, siis saab äkki need ka mapitud
-- kokku 1039
create table analysiscleaning.unmaped_parameter_code_to_t_lyhend as
select distinct parameter_code_raw, count(*) from work.run_ac_html_201901171409_analysis_entry
where parameter_code_raw not in
    --eelenv kood
      (select distinct e.parameter_code_raw
      from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as a
          right join work.run_ac_html_201901171409_analysis_entry as e
              on lower(a.dirty_code) = lower(e.parameter_code_raw)
      where dirty_code is not null
      )
     and not (parameter_code_raw  ~ '^[0-9]*$')
group by parameter_code_raw
order by count desc;
--ülemise koodi panengi notebooki



-- siia lükkan kõik puhastatud 'analysiscleaning.dirty_code_to_t_lyhed_mapping_all_CLEAN'
drop table if exists analysiscleaning.dirty_code_to_t_lyhed_mapping_all_clean;
create table analysiscleaning.dirty_code_to_t_lyhed_mapping_all_clean (
  dirty_code varchar,
  clean_code varchar,
  t_lyhend varchar
);


-- pythonis täitsime ära tabeli
--nüüd vaatame, kas parandas meie mappingut, vaatame palju meie puhastatud parameter_code'ga saab mapitud entry tabelist

--lisame puhastatud nimedele juurde t_lyhendid
update analysiscleaning.dirty_code_to_t_lyhed_mapping_all_clean as clean
set t_lyhend = a."loinc_code(T-luhend)"
    from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as a
    where lower(clean.clean_code) = lower(a.dirty_code) or lower(clean.clean_code) = lower(a."loinc_code(T-luhend)")
-- 69 lisarida saime mapitud, nüüd 124 988st?

--tahame teada palju meie unmaped'st sai mapitud
select u.parameter_code_raw, clean.clean_code, clean.t_lyhend, u.count from analysiscleaning.unmaped_parameter_code_to_t_lyhend as u
left join analysiscleaning.dirty_code_to_t_lyhed_mapping_all_clean as clean
    on u.parameter_code_raw = clean.dirty_code
where t_lyhend is not null;
--saime 111 lsiarida mapitud, mis enne olid mappimata, saime 121??

--TEGELT SIIN VÕIKS CLEANITUD NIMED OLLA!!!!!!!!!!!!!!!!!!!!!!!!!!
-- olgu meil tabel parameter_code_to_t_lyhend_mapping_entry, kus on kõik entrys esinevad parameter_coded populaarsuse järgi
-- ja neile vastavad t_lyhendid
drop table if exists analysiscleaning.parameter_code_to_t_lyhend_mapping_entry;
create table analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as
select distinct e.parameter_code_raw, a."loinc_code(T-luhend)", e.count
from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as a
right join
      (select distinct parameter_code_raw, count(*)
      from work.run_ac_html_201901171409_analysis_entry
      where not (parameter_code_raw  ~ '^[0-9]*$')
      group by parameter_code_raw
      order by count desc) as e
on lower(a.dirty_code) = lower(e.parameter_code_raw) or lower(a."loinc_code(T-luhend)") = lower(e.parameter_code_raw)
order by count desc;
--algselt 159 mapitud, nüüd 165, nüüd 181

--lisame puhta PC jaoks veeru
alter table analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
add column parameter_code_clean varchar;

--lisame puhtad PC nimed
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as e
set parameter_code_clean = c.clean_code
    from analysiscleaning.dirty_code_to_t_lyhed_mapping_all_clean as c
    where lower(c.dirty_code) = lower(e.parameter_code_raw);


--lisame sinna ka puhastatud nimedes saadud t_lyhendid tulemused
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as e
set "loinc_code(T-luhend)" = clean.t_lyhend
    from analysiscleaning.dirty_code_to_t_lyhed_mapping_all_clean as clean
    where (lower(clean.dirty_code) = lower(e.parameter_code_raw)
          or lower(clean.clean_code) = lower(e.parameter_code_raw))
          and clean.t_lyhend is not null and e."loinc_code(T-luhend)" is null;
-- nüüd kokku 263 mapitud, nüüd 268, nüüd 294

--lisame puhastatud nimed, kui puhastamine seisnes millegis enamas kui lihtsalt lowercaseks tegemises
/*update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as e
set parameter_code_raw = clean.clean_code
    from analysiscleaning.dirty_code_to_t_lyhed_mapping_all_clean as clean
    where lower(clean.dirty_code) = lower(e.parameter_code_raw)
          and lower(clean.clean_code) != lower(e.parameter_code_raw)*/
;
-- nüüd kokku 263 mapitud, nüüd 268

--lisame ka classificationsis olevad t_lyhendid
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as e
set "loinc_code(T-luhend)" = a.t_lyhend
    from classifications.elabor_analysis as a
    where (lower(a.t_lyhend) = lower(e.parameter_code_raw) or
          lower(a.t_lyhend) = lower(e.parameter_code_clean)) and
          e."loinc_code(T-luhend)" is null;
-- nüüd kokku 303 mapitud, nüüd 325, nüüd 343

-- see katab ära 13898/26877 = 52% entry kirjetest
-- nüüd 14150/ (14150+12118) = 53%
-- nüüd 19 990 / 26877 (siin ka juba uus reelgite fail complete_manual sees)
select sum(count) from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
where "loinc_code(T-luhend)" is not null;

-- loodud uute reeglite faili latest_completely_manual_case rakendamine
select e.parameter_code_raw, m."loinc_code(T-luhend)" from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as e
left join analysiscleaning.latest_completely_manual_cases as m
on e.parameter_code_raw = m.dirty_code
where m."loinc_code(T-luhend)" is not null;

/*
--nüüd mapitud 346, juba sees  dirty_code_to_t_lyhed_mapping_all's
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as e
set "loinc_code(T-luhend)" = m."loinc_code(T-luhend)"
    from analysiscleaning.latest_completely_manual_cases as m
    where lower(m.dirty_code) = lower(e.parameter_code_raw);
*/

-- entry kõigist sõnalistest parameter_code ridadest mapime ära 20677, mappimata jääb 5591
-- katame ära 20833/26268 = 79% (sõnalistest!!!)
select sum(count) from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
where "loinc_code(T-luhend)" is not null;

-- kõigist parameter_code'st katame ära (ka numbrilised) 20677/66814 = 31%
select sum(count)
from (
      select distinct parameter_code_raw, count(*) from work.run_ac_html_201901171409_analysis_entry
      group by parameter_code_raw
     ) as t;

-------------------------------------------------------------------------------------------------------------
/*
  PARAMETER NAME
*/
--------------------------------------------------------------------------------------------------------------
-- vaja oleks cleaninda parameter_name'd ja siis teha mapping
-- osa PN on juba varasemate reeglite põhjal mapitavavd
-- jagame distinct PN kahte tabelisse
-- 1. kus kõigi vanade reeglite poolt mapitud t_lyhend
-- 2. kus PN ei oska t_lyhendit määrata, aga seal cleanime/klasterdame PN ära ja siis lisame t_lyhendi

-- 1. juba ära puhastatud PN
drop table if exists analysiscleaning.parameter_name_to_t_lyhend_mapping;
create table analysiscleaning.parameter_name_to_t_lyhend_mapping as
-- elabori järgi saab osa PN ära mappida t_lyhendi peale
select distinct e.parameter_name_raw, a.t_lyhend from classifications.elabor_analysis as a
  right join work.run_ac_html_201901171409_analysis_entry as e
    on a.t_lyhend =  e.parameter_name_raw
    where a.t_lyhend is not null;
/*
--täiendame mappingut olemasolevate reeglite põhjal(cda-data-cleaning/analysis_data_cleaning/name_cleaning/data)
-- 70 PN saab mapitud
insert into analysiscleaning.parameter_name_to_t_lyhend_mapping (parameter_name_raw, t_lyhend)
select distinct e.parameter_name_raw, p."loinc_code(T-luhend)" from analysiscleaning.latest_prefix_cases as p
    right join  work.run_ac_html_201901171409_analysis_entry as e
    on p.dirty_code = e.parameter_name_raw
    where dirty_code is not null;

-- 6 PN saab mapitud
insert into analysiscleaning.parameter_name_to_t_lyhend_mapping (parameter_name_raw, t_lyhend)
select distinct e.parameter_name_raw, p."loinc_code(T-luhend)" from analysiscleaning.latest_semimanual_cases as p
    right join  work.run_ac_html_201901171409_analysis_entry as e
    on p.dirty_code = e.parameter_name_raw
    where dirty_code is not null;

-- 9 PN saab mapitud
insert into analysiscleaning.parameter_name_to_t_lyhend_mapping (parameter_name_raw, t_lyhend)
select distinct e.parameter_name_raw, p."loinc_code(T-luhend)" from analysiscleaning.latest_name_cases as p
    right join  work.run_ac_html_201901171409_analysis_entry as e
    on p.dirty_code = e.parameter_name_raw
    where dirty_code is not null;

-- 15 PN saab mapitud siin vaja unitit ka!!
insert into analysiscleaning.parameter_name_to_t_lyhend_mapping (parameter_name_raw, t_lyhend)
select distinct e.parameter_name_raw, p."loinc_code(T-luhend)" from analysiscleaning.latest_subspecial_cases as p
    right join  work.run_ac_html_201901171409_analysis_entry as e
    on p.dirty_code = e.parameter_name_raw and p.unit = e.parameter_unit
    where dirty_code is not null;

-- 42 PN saab mapitud siin vaja unitit ka!!
insert into analysiscleaning.parameter_name_to_t_lyhend_mapping (parameter_name_raw, t_lyhend)
select distinct e.parameter_name_raw, p."loinc_code(T-luhend)" from analysiscleaning.latest_parentheses_one_cases as p
    right join  work.run_ac_html_201901171409_analysis_entry as e
    on p.dirty_code = e.parameter_name_raw
    where dirty_code is not null;

-- 9 PN saab mapitud siin vaja unitit ka!!
insert into analysiscleaning.parameter_name_to_t_lyhend_mapping (parameter_name_raw, t_lyhend)
select distinct e.parameter_name_raw, p."loinc_code(T-luhend)" from analysiscleaning.latest_parentheses_two_cases as p
    right join  work.run_ac_html_201901171409_analysis_entry as e
    on p.dirty_code = e.parameter_name_raw
    where dirty_code is not null;

-- 0 PN saab mapitud siin vaja unitit ka!!
insert into analysiscleaning.parameter_name_to_t_lyhend_mapping (parameter_name_raw, t_lyhend)
select distinct e.parameter_name_raw, p."loinc_code(T-luhend)" from analysiscleaning.latest_parentheses_three_cases as p
    right join  work.run_ac_html_201901171409_analysis_entry as e
    on p.dirty_code = e.parameter_name_raw
    where dirty_code is not null;

--TULEMUS 161 parameter_name saab automaatselt mapitud t_lyhendiks ehk need on triviaalsed
-- punkt 2. vaja hakata ÜLEJÄÄNUSID PN ehk 1425 - 161 = 1264 PN ühtlustama
*/
-----------------------------------------------------------------------------------------
--2. kus PN ei oska t_lyhendit määrata, aga seal cleanime/klasterdame PN ära ja siis lisame t_lyhendi
-- VÕI määrame PN ära PC ja kui PC on mappitud, saame ka t_lyhendi

-- paneme parameter_code järgi t_lyhendid
-- parameter_code kasutamine ei paranda meie tulemust,
--  sest eelnevate reeglite põhjal said juba kõik mapitud tabelisse parameter_name_to_t_lyhend_mapping
select distinct e.parameter_name, e.parameter_code_raw, p.t_lyhend from work.run_ac_html_201901171409_analysis_entry as e
left join analysiscleaning.parameter_code_to_t_lyhend_mapping as p
    on e.parameter_code_raw = p.parameter_code
where parameter_name is not null and
      t_lyhend is not null and
      parameter_name not in (select parameter_name from analysiscleaning.parameter_name_to_t_lyhend_mapping)

-- vaatleme nüüd parameter_name, mis mappimata jäänud
select distinct e.parameter_name from work.run_ac_html_201901171409_analysis_entry as e
where e.parameter_name not in (select t.parameter_name_raw from analysiscleaning.parameter_name_to_t_lyhend_mapping as t)

-- mappimata jäänud parameter_name'd saadame jupyteri notebooki cleanimiseks,
-- anname kaasa ka parameter_code'i ja ocounti, et prioritiseerida
select distinct e.parameter_name, e.parameter_code_raw, count(*) from work.run_ac_html_201901171409_analysis_entry as e
where e.parameter_name not in (select t.parameter_name_raw from analysiscleaning.parameter_name_to_t_lyhend_mapping as t)
group by parameter_name, parameter_code_raw;
-- kasutame seda koodi notebookis query = (see kood)

-- näeme, et saaks tulemust parandada, kui entrys olevad parameter_name'd normaliseerida nagu MedAnalysis klassis
-- e.parameter_name         -> defineeritud reeglites parameter_name kuju
-- B-Mono#(Monotsüütide arv) ->     B-Mono#(Monotsuutide arv)
-- B-Mono%(Monotsüütide suhtarv) -> B-Mono%(Monotsuutide suhtarv)
-- B-Mono(Monotsüütide arv) ->      B-Mono(Monotsuutide arv)
-- B-Mono(Monotsüütide suhtarv)
-- sama ka leukotsüütide ja muude tsüütide puhul

--veel saaks tulemust parandada kui parameter_name
--S-Na (naatrium) --> S-Na
-- S-K (kaalium) --> S-K

-- jääb mulje, et seda ongi tehtud clean_value ja cleaning.py abil



SELECT REPLACE('Monotsüütide','ü','u');
/*
select distinct e.parameter_name, e.parameter_code_raw from work.run_ac_html_201901171409_analysis_entry as e
where e.parameter_name not in (select t.parameter_name_raw from analysiscleaning.parameter_name_to_t_lyhend_mapping as t)
   and not (parameter_code_raw  ~ '^[0-9]*$');*/






--
/*select * from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where "loinc_code(T-luhend)" like '%Gluc%'

select *
from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where "loinc_code(T-luhend)" like '%B-CBC%';

select *
from classifications.elabor_analysis
where t_lyhend like '%B-CBC%';
*/
--kas "_" eemaldamine PC aitaks?
-- U-Bil_strip
select * from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where dirty_code like 'U-Bil%';

select * from classifications.elabor_analysis
where t_lyhend like 'U-Bil%'

select * from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
where parameter_code_raw like 'U-Bil%'

-- entry tabelist mappiTUD kokku 13898 PC rida, nüüd 14150
select sum(count) from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
where "loinc_code(T-luhend)" is not null;
-- entry tabelist mappiMATA kokku 12118 PC rida
select sum(count) from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
where "loinc_code(T-luhend)" is null;

--kui mappida KÄSITSI NT ära kõige populaarsemad 20, siis saame juurde 6908 mapitud PC rida
select sum(count) from
      (select * from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
      where "loinc_code(T-luhend)" is null
      limit 20) as t;






-----------------------------------------------------------------------------------
-- KÄSITSI mapping top 20 mappimata PC-le
-- praegu mapib PC ka paneele või mitmeselt
-- lõin kõige järgneva põhjal !!!latest_completely_manual_cases.csv!!, kus saab kõiki neid reegleid korraga rakendada

select * from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
where "loinc_code(T-luhend)" is null
limit 20;
--parameter_code_raw: aB-ABB,B-CBC+Diff,cB-Gluc-p,B1-A3Diff,B-A3Diff,aB-Elektrolüüdid,B-Hemo,HGB,B-CBC-Diff,B-Gluc,
--U-Sedim,aB-tO2,aB-ABE,Laktaat_arteriaalses_plasmas,Kaalium_arteriaalses_plasmas,Naatrium_arteriaalses_plasmas,
--Glükoos_arteriaalses_veres,LCR,LYMPH#,LYMPH%

--SUBJEKTIIVNE
--PC: 'aB-ABB'
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'aB-ABB panel'
where parameter_code_raw = 'aB-ABB';

update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = ('B-CBC-3Diff','B-CBC-5Diff')
where parameter_code_raw = 'B-CBC+Diff';

--PC: 'cB-Gluc-p', AN: Korduv glükoos kapillaarverest
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'fcB-Gluc'
where parameter_code_raw ='cB-Gluc-p';

--????? PN: B1-A3Diff ja  B-A3Diff AN: Vere automaatuuring 3-osalise leukogrammiga
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'B-CBC-3Diff'
where parameter_code_raw = 'B1-A3Diff';

update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'B-CBC-3Diff'
where parameter_code_raw = 'B-A3Diff';

-- PC: aB-Elektrolüüdid AN:Kaltsium, Naatrium, Kaalium
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'aP-Electrolytes panel'
where parameter_code_raw = 'aB-Elektrolüüdid';

--PC: B-Hemo, AN: Vereäige mikroskoopia
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'B-Smear-m panel'
where parameter_code_raw = 'B-Hemo';

-- PC: HGB, AN: erinevaid
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'B-Hb'
where parameter_code_raw = 'HGB';

update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = ('B-CBC-3Diff','B-CBC-5Diff')
where parameter_code_raw = 'B-CBC-Diff';

-- PC: B-Gluc AN: Glükoos täisveres (glükomeetriga)
----fB-Gluc tundub õige, aga seda pole elaboris, fcB-Gluc on olemas, aga see PAASTUVERES (söömata võetud veri)
--- täisvere ikkagi erinev kui seerumis/plasmas
--- ??????

--PC: U-Sedim AN:Uriini sademe mikroskoopia,
-- KAS U-Sed-m panel VÕI U-Sed-m???
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'U-Sed-m panel'
where parameter_code_raw = 'U-Sedim';

-- PC: aB-tO2, AN:Happe-aluse tasakaalu uuring arteriaalses veres
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'aB-O2'
where parameter_code_raw = 'aB-tO2';

-- PC:aB-ABE AN:Happe-aluse tasakaalu uuring arteriaalses veres
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'aB-BE'
where parameter_code_raw = 'aB-ABE';

-- PC: Laktaat_arteriaalses_plasmas AN: Laktaat arteriaalses plasmas
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'aP-Lac'
where parameter_code_raw = 'Laktaat_arteriaalses_plasmas';

-- PC: Kaalium_arteriaalses_plasmas, AN: Kaalium arteriaalses plasmas
-- peaks olema aP-K, aga seda elaboris pole
-- või käib S,P-K alla??

-- PC: Naatrium_arteriaalses_plasmas, AN: Naatrium arteriaalses plasmas
-- peaks olema aP-Na, aga seda elaboris pole
-- ???


-- PC: Glükoos_arteriaalses_veres, AN: Glükoos arteriaalses veres
-- võiks olla nt aB-Gluc, aga sellist pole elaboris

-- PC: LCR, AN: CBC Hemogramm / CBC-5Diff Hemogramm viieosalise leukogrammiga /Kliiniline vereanalüüs
update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'B-LCR'
where parameter_code_raw = 'LCR';

update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'B-Lymph#'
where parameter_code_raw = 'LYMPH#';

update analysiscleaning.parameter_code_to_t_lyhend_mapping_entry
set "loinc_code(T-luhend)" = 'B-Lymph%'
where parameter_code_raw = 'LYMPH%';

--saame 78% ära katta


--fB-Gluc VÕI fcB-Gluc
