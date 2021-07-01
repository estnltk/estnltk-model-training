--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  -- puhas kood failis parameter_name_to_loinc_code(T-luhend)_mapping.sql
-------------------------------------------------------------------------------
  -- RUNFULL mappimine PN -> T_lyh
-------------------------------------------------------------------------------
-- runfull PN järgi mappimine
drop table if exists  analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html;

--teeme koopia analysis_runfull tabelist, koos veeruga LOINCI jaoks
create table analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as
  select *
  from work.runfull201903041255_analysis_html;

ALTER TABLE analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
ADD COLUMN "loinc_code(T-luhend)" VARCHAR

------------------------------------------
  -- PUHASTAMINE
 ----------------------------------------
-- 1.1 kõigepealt PUHASTAME sellised PN tüüpid ära:
/*
a2034 WBC
a2035 RBC
a2041 Hb
*/
--neid tuvastav regex: '^[a]\d{4}'
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substring(parameter_name_raw, 7)
where parameter_name_raw~'^[a]\d{4}';

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substring(parameter_name_raw, 6)
where parameter_name_raw~'^[a]\d{3}';

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substring(parameter_name_raw, 5)
where parameter_name_raw~'^[a]\d{2}';

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substring(parameter_name_raw, 4)
where parameter_name_raw~'^[a]\d{1}';


select substring(parameter_name_raw, 5), parameter_name_raw, count(*)
from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  where parameter_name_raw ~ '^[a]\d{1}'
group by parameter_name_raw
order by count desc;

---------------------------------------------------------------------------------------------
-- 1.2 Puhastame seda sorti:
/*
Fibrinogeen plasmas* -> Fibrinogeen plasmas
Kolesterool seerumis* -> Kolesterool seerumis
Prokaltsitoniin seerumis* -> Prokaltsitoniin seerumis
*/

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = substr(parameter_name_raw, 1, length(parameter_name_raw) - 1)
where parameter_name_raw like '%*'

--select distinct parameter_name_raw,  substr(parameter_name_raw, 1, length(parameter_name_raw) - 1) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
--where parameter_name_raw like '%*'

---------------------------------------------------------------------------------
 -- 1.3 ühekordsete sulgude eemaldus
---------------------------------------------------------------------------------

-- ei taha sulgudes olevat teksti täiesti kaotada,
  -- nii et selle jaoks vanad väärtused paneme veergu parameter_name ja uued parameter_name_raw
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
  set parameter_name = parameter_name_raw;

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = regexp_replace(parameter_name_raw, '\(.*?\)','')
where  parameter_name_raw ~ '(.*)\((.*)\)';

select distinct parameter_name_raw, parameter_name from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where  parameter_name ~ '(.*)\((.*)\)';
/*
select distinct parameter_name_raw,
                  regexp_replace(parameter_name_raw, '\(.*?\)','') as a,
                  regexp_replace(parameter_name_raw, '\(.*?\).\)','') as s,
                  regexp_replace(parameter_name_raw, '(.*)\((.*)\)', '')
                  --regexp_replace(parameter_name_raw,'\([()]*(\([^()]*\)[^()]*)*\)', '')  as b
                  --regexp_replace(parameter_name_raw, '^(.*)\\(.*?\\)', '\\1'),
                  --regexp_replace(parameter_name_raw, '\\(.*?\\)$', '')
  from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
    where parameter_name_raw ~ '(.*)\((.*)\)';
*/

-------------------------------------------------------------------------
--1.4 märgid % ja # sõna lõppu:  %NEUT -> NEUT%, #NEUT->NEUT#
  ------------------------------------------------------------------------
-- %LYMPH -> LYMPH%
/*select parameter_name_raw, SUBSTRING(parameter_name_raw, 2, 8000) || '%'
from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where parameter_name_raw LIKE '\%%';*/
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = SUBSTRING(parameter_name_raw, 2, 8000) || '%'
where parameter_name_raw LIKE '\%%';

--
--#LYMPH -> LYMPH#
/*select parameter_name_raw, SUBSTRING(parameter_name_raw, 2, 8000) || '#'
from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where parameter_name_raw LIKE '\#%';*/
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = SUBSTRING(parameter_name_raw, 2, 8000)  || '#'
where parameter_name_raw LIKE '\#%';


--------------------------------------------------------------------------------
 -- REMOVE WHITESPACE
  -- kui on rohkem kui üks whitespace
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = trim(regexp_replace(parameter_name_raw, '\s+', ' ', 'g'));


---------------------------------------------------------------------------------
  -- Hakkame  PN -> T_LYHENDIT MAPPIMA
---------------------------------------------------------------------------------
-- Step 2.1
  -- 2.1.1
--------------------------------------------------------------------------------
-- ELABORIGA PARANDAMINE

-- kasutame elabor_analysis et mappingut parandada, sest nt seal on kasutatav_nimetus MCH mis on samas ka parameter_name
-- jas seega saame selle abil t_lyhendi ära parandad
-- ehk kui LOINC pole veel mapitud üritame seda mappida elabori abil

--kasutatava nimetusega mapping
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = t_lyhend
from classifications.elabor_analysis as e
where t."loinc_code(T-luhend)" is null and
      upper(e.kasutatav_nimetus) = upper(t.parameter_name_raw);

-- mappimata ridu nüüd 2798544 ehk 2798544/4 246 711 65.9%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

-- t_lyhendi endaga mapping
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = t_lyhend
from classifications.elabor_analysis as e
where "loinc_code(T-luhend)" is null and
      upper(e.t_lyhend) = upper(t.parameter_name_raw);

-- mappimata ridu nüüd 2538759 ehk 2538759/4 246 711 59.8%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

-- t_nimetusega mapping
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = t_lyhend
from classifications.elabor_analysis as e
where "loinc_code(T-luhend)" is null and
      upper(e.t_nimetus) = upper(t.parameter_name_raw);

-- mappimata ridu nüüd  2476782 ehk 2476782/4 246 711 = 58.3%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

--kõige sagedamini esinev mappimata PN
select parameter_name_raw, count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null
group by parameter_name_raw
order by count desc;

-------------------------------------------------------------------------
-- 2.1.2
  -- MAPPIMISE PARANDAMISE CSV FAILI LOOMINE
-- Elaboris:  tlyhend + t_nimetus ->  t_lyhendiks
  -------------------------------------------------------------
-- tlyh_and_t_nimetus_to_tlyhend
drop table if exists analysiscleaning.dirty_code_tlyh_and_t_nimetus_to_tlyhend;
create table analysiscleaning.dirty_code_tlyh_and_t_nimetus_to_tlyhend as
select t_lyhend || ' ' || t_nimetus AS tlyh_ja_nimetus, t_lyhend as "loinc_code(T-luhend)"
from classifications.elabor_analysis;

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = a."loinc_code(T-luhend)"
from analysiscleaning.dirty_code_tlyh_and_t_nimetus_to_tlyhend as a
where t."loinc_code(T-luhend)" is null and
      upper(a.tlyh_ja_nimetus) = upper(t.parameter_name_raw);

-- mappimata ridu nüüd 2459122 ehk 2459122/4 246 711 = 57.9%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

--kõige sagedamini esinev mappimata PN
select parameter_name_raw, count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null
group by parameter_name_raw
order by count desc;


---------------------------------------------------------------------------------------
-- 2.2 Replacing special characters
  -- täpitähtede eemaldus
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set parameter_name_raw = translate(parameter_name_raw, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U');

-------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------
-- 2.3 Predefined csv file rules
  -- 2.3.1
-- mapping failis on PN -> T_LYHEND, mis põhineb  latest... failidel and put together in parameter_name_to_tlyhend.sql
------------------------------------------------------------------------------------
--() cleaniTUD PN järgi
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = dc."loinc_code(T-luhend)"
from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as dc
where t."loinc_code(T-luhend)" is null and
      upper(dc.dirty_code) = upper(t.parameter_name_raw);

-- mappimata ridu nüüd  1298402 ehk 1298402/4 246 711 = 30.6%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;


-- () cleaniMATA PN järgi
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = dc."loinc_code(T-luhend)"
from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as dc
where t."loinc_code(T-luhend)" is null and
      upper(dc.dirty_code) = upper(t.parameter_name);


-- mappimata ridu nüüd  1272774 ehk 1272774/4 246 711 = 30.0%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;


  /* ÕIGE JA HEA
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = pn."loinc_code(T-luhend)"
from analysiscleaning.parameter_name_to_t_lyhend_mapping as pn
where t."loinc_code(T-luhend)" is null and
      pn."loinc_code(T-luhend)" is not null and
      upper(pn.parameter_name_raw) = upper(t.parameter_name_raw);
--mappimata ((1315174)))

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = pn."loinc_code(T-luhend)"
from analysiscleaning.parameter_name_to_t_lyhend_mapping as pn
where t."loinc_code(T-luhend)" is null and
      pn."loinc_code(T-luhend)" is not null and
      upper(pn.parameter_name_clean) = upper(t.parameter_name_raw);
--
-- mappimata ridu 1315174 ehk  1 315 174/ 4 246 711 = 31.0%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

--kõige sagedamini esinev mappimata PN
select parameter_name_raw, count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null
group by parameter_name_raw
order by count desc;
 */

--------------------------------------------------------------------------------



--------------------------------------------------------------------------------------------------
-- DIRTY_CODE_TO_TLYHEND ABIL PARANDAMINE, NB! POLE TARVIS
---------------------------------------------------------------------------------------
-- kasutame varem loodud faili dirty_code_to_t_lyhend et parandada analysis_html mappingut
-- EI PARANDA MAPPINGUT SEST parameter-name_to_tlyhend JUBA SISALDAB DIRTY_CODE TO TLYHEND
/*update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = d."loinc_code(T-luhend)"
from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as d
where t."loinc_code(T-luhend)" is null and
      upper(d.dirty_code) = upper(t.parameter_name_raw);

-- mappimata ridu nüüd 1634174 ehk 1634174/4 246 711 = 39%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

--kõige sagedamini esinev mappimata PN
select parameter_name_raw, count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null
group by parameter_name_raw
order by count desc;*/






------------------------------------------
-- 2.3.1 Mapping by parameter_name and unit
-- PN ja UNITi järgi mappimine
-- tabel latest_subspecial_cases
---------------------------------------

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = s."loinc_code(T-luhend)"
from analysiscleaning.latest_subspecial_cases as s
where upper(s.dirty_code) = upper(t.parameter_name_raw) and
      upper(s.unit) = upper(t.parameter_unit_raw);

-- mappimata ridu nüüd 1252487 ehk 1252487/4 246 711 = 29.5%
-- uus 1278185
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

--kõige sagedamini esinev mappimata PN
select parameter_name_raw, count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null
group by parameter_name_raw
order by count desc;

------------------------------------------------------------------------------------------------
-- 3
--kui PN on null -> mappida AN järgi
-----------------------------------------------------------------------------------------------
select analysis_name_raw, parameter_name_raw
from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where parameter_name_raw is null;


update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = t_lyhend
from classifications.elabor_analysis as e
where parameter_name_raw is null and
      "loinc_code(T-luhend)" is null and
      upper(e.kasutatav_nimetus) = upper(t.analysis_name_raw);

-- mappimata ridu nüüd 1190143 ehk 1190143/4 246 711 = ..%
--uus 1215841
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;


update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = t_lyhend
from classifications.elabor_analysis as e
where parameter_name_raw is null and
      "loinc_code(T-luhend)" is null and
      upper(e.t_lyhend) = upper(t.analysis_name_raw);

-- mappimata ridu nüüd 1187512 ehk 1187512/4 246 711 =
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = t_lyhend
from classifications.elabor_analysis as e
where parameter_name_raw is null and
      "loinc_code(T-luhend)" is null and
      upper(e.t_nimetus) = upper(t.analysis_name_raw);

-- mappimata ridu nüüd 1173619 ehk 1173619/4 246 711 = 27.6%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;



----------------------------------------------------------------------------------------------
-- Repeat step 2.2 on AN
  --AN puhastus
update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
set analysis_name_raw = translate(analysis_name_raw, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U')
where parameter_name_raw is null;

--------------------------------------------------------------------------------------------
-- AN järgi mappimine

update analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html as t
set "loinc_code(T-luhend)" = pn."loinc_code(T-luhend)"
--from analysiscleaning.parameter_name_to_t_lyhend_mapping as pn
from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as pn
where t.parameter_name_raw is null and
      pn."loinc_code(T-luhend)" is not null and
      t."loinc_code(T-luhend)" is null and
      upper(pn.dirty_code) = upper(t.analysis_name_raw);

-- mappimata ridu nüüd 1151548 ehk 1151548/4 246 711 =27.1%
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null;


--kõige sagedamini esinev mappimata PN
select parameter_name_raw, count(*)
from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null
group by parameter_name_raw
order by count desc;


-- kõige sagedamini mappimata PN = NULL AN
select analysis_name_raw, parameter_name_raw, count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null and parameter_name_raw is null
group by parameter_name_raw, analysis_name_raw
order by count desc;










---------------------------------------------
select count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null and parameter_name_raw like '%)'
--group by parameter_name_raw
order by count desc;



select * from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where dirty_code like '%P-LDL-Chol%'

select *
from classifications.elabor_analysis
where upper(t_lyhend) like '%S-N%';

select parameter_name_raw, parameter_name,  "loinc_code(T-luhend)" from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where parameter_name_raw like 'S-LDL-Chol%';

--S-ASAT  S-Chol  S-LDL-Chol
----------------------------------------------------------------
-- mappimata PN
select parameter_name_raw, count(*) from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where "loinc_code(T-luhend)" is null
group by parameter_name_raw
order by count desc;

select * from analysiscleaning.t_lyhend_mapping_to_runfull201903041255_analysis_html
where parameter_name_raw is null

select * from analysiscleaning.parameter_name_to_t_lyhend_mapping
where parameter_name_raw like 'P-ASAT%'

select * from classifications.elabor_analysis
where t_nimetus like '%Hemoglobiin';




/*
Varvus
U-Bil Bilirubiin
U-Ubg Urobilinogeen
U-pH Happe-aluse tasakaal
U-WBC Leukotsuudid
U-Glu Glukoos
U-RBC Erutrotsuudid
U-Pro Valk
U-Ket Ketokehad
U-Col Varvus
U-Cla Labipaistvus

*/



select * from analysiscleaning.parameter_name_to_t_lyhend_mapping
where parameter_name_raw like '%umfotsuutide suht%'


select * from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where dirty_code like '%umfotsuutide suht%'
/*Lumfotsuutide suhtarv
Lumfotsuutide arv
Monotsuutide suhtarv
Monotsuutide arv
*/




select * from classifications.elabor_analysis
where t_nimetus like '%C-reakt%'

select * from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where "loinc_code(T-luhend)" like '%CRP%'



--HGB Hemoglobiin
--WBC Leukotsüüdid
/*
lisa latest_prefix_cases!!
IG% Ebaküpsete granulotsüüdide suhtarv
RDW-CV Erütrotsüütide anisotsütoosi näitaja


*/
/*
,151450
fP-Crea (kreatiniin),7040
TSH / Türeotropiin,6925
Segmenttuumsed neutrofiilid,6381
ESR Erütrotsüütide settekiirus,6346
P-ALAT alaniini aminotransferaas,6151
MID,6085
P-PT-INR - Protrombiini aeg- rahvusvaheline normitud suhe,5873
P-K kaalium,5777
B-Plt(Trombotsüütide arv),5749
B-RDW(Erütrotsüütide suurusjaotuvus),5513
fP-Urea (uurea),5487
B-Gluc,5393
U-SG(Uriini erikaal),5146
U-Leu(Leukotsüüdid uriinis),5143
U-Ery(Erütrotsüüdid uriinis),5125
U-Glu(Glükoos uriinis),5122
B-HFLC(High fluorescence lymphocytes),5055
FT4 / Vaba türoksiin,5010
Nitritid,4891
B-HGB,4803
PLT(Trombotsüütide arv),4741
MCH(Keskmine hemoglobiini hulk erütrotsüüdis),4676
MCHC(Keskmine hemoglobiini konsentratsioon erütrotsüüdis),4672
Värvus,4624
U-Nit Nitrit,4416
U-Bil Bilirubiin,4416
U-Ubg Urobilinogeen,4416
U-pH Happe-aluse tasakaal,4414
U-WBC Leukotsüüdid,4413
U-Glu Glükoos,4407
U-RBC Erütrotsüüdid,4406
U-Pro Valk,4406
U-Ket Ketokehad,4405
U-Col Värvus,4403
U-Cla Läbipaistvus,4397
U-SG Erikaal,4331
Ketoonid,4330
C-reaktiivne valk,4264
PDW(Trombotsüütide jaotuvus mahujärgi. e. trombotsüütide anisotsütoosimäär),4220
fP-Gluc plasma Glükoos plasmas,4033
Lameepiteel,3869
Leukotsüüdid veres 10E9/l,3832
Erütrotsüüdid veres 10E12/l,3831
Hemoglobiin veres g/l,3829
MCH pg,3816
Trombotsüüdid 10E9/l,3816
RDW %,3816
MCHC g/l,3816
MCV fl,3816
MPV fl,3807

*/