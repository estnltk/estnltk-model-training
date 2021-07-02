-- KAS on olukordi, kui PN -> PC1 -> t_lyh1 ja samal ajal PN -> PC2 -> t_lyh2?
-- kuna numbrilistele PC ei leidu -> t_lyhendit, siis nendega konflikti tekkida ei saa
-- ehk EI, POLE OLUKORDI KUS PN -> PC1, PC2
select parameter_name, array_agg(parameter_code_raw), count(*)
from analysiscleaning.parameter_name_to_parameter_code_mapping
where not (parameter_code_raw  ~ '^[0-9]*$')
group by parameter_name
order by count desc;

---------------------------------------------------------------------------------------------
-- 1. PN -> t_lyh
-- 2. PN -> PC -> t_lyh
-- vaatame kas langevad kokku/palju neil ühist
---------------------------------------------------------------------------------------------

-- 1. PN-> t_lyh


-- mapib ära 7861 rida (66814st??)
select p."loinc_code(T-luhend)", e.analysis_name, e.parameter_name_raw, e.parameter_code_raw from work.run_ac_html_201901171409_analysis_entry as e
left join analysiscleaning.parameter_name_to_t_lyhend_mapping as p
    on lower(e.parameter_name) = lower(p.parameter_name) or
       lower(e.parameter_name) = lower(p.parameter_name_clean)
where "loinc_code(T-luhend)" is not null;

--------------------------------------------------------------------------------------------------------

-- 2. PN -> PC -> t_lyhend

-- mapib ära 23628 rida
-- mappimata jääb 48217
select p."loinc_code(T-luhend)", e.analysis_name, e.parameter_name_raw, e.parameter_code_raw
from work.run_ac_html_201901171409_analysis_entry as e
left join analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as p
    on lower(e.parameter_code_raw) = lower(p.parameter_code_raw)
where "loinc_code(T-luhend)" is not null;


----------------------------------------------------------------------------------------------------------------------
-- teen uue entry tabeli, kus lisan nii PN abil kui PC abil mapitud t_lyhendi
drop table if exists analysiscleaning.comparing_pc_pn_mappings_on_entry;
create table analysiscleaning.comparing_PC_PN_mappings_on_entry as
select p."loinc_code(T-luhend)" as t_lyhend_PN, e.analysis_name, e.parameter_name, p.parameter_name_clean, e.parameter_code_raw, e.parameter_unit
from work.run_ac_html_201901171409_analysis_entry as e
left join analysiscleaning.parameter_name_to_t_lyhend_mapping as p
    on lower(e.parameter_name) = lower(p.parameter_name)
       --lower(e.parameter_name) = lower(p.parameter_name_clean);


-- cleanitud nimede järgi PN mapping
update analysiscleaning.comparing_PC_PN_mappings_on_entry  as e
set t_lyhend_pn = p."loinc_code(T-luhend)"
  from analysiscleaning.parameter_name_to_t_lyhend_mapping as p
  where lower(e.parameter_name) = lower(p.parameter_name_clean) and
    t_lyhend_pn is null;
--7861 PN mappitud

--teeme uue veeru PC abil mapitud t_lyhenditele
alter table analysiscleaning.comparing_PC_PN_mappings_on_entry
  --add parameter_code_clean varchar,
  add t_lyhend_PC varchar;

--mapime PC järgi ära, kasutades 1. puhastatud PC 2. miite puhast PC
--1.
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_pc = p."loinc_code(T-luhend)"
 from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as p
  where lower(e.parameter_code_raw) = lower(p.parameter_code_raw);
--2.
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_pc = p."loinc_code(T-luhend)"
 from analysiscleaning.parameter_code_to_t_lyhend_mapping_entry as p
  where lower(e.parameter_code_raw) = lower(p.parameter_code_clean) and
        e.t_lyhend_PC is null;

--19 286, nüüd 21418 PC mapitud

-- kas on konflikte, kus PC ja PN mapivad erienvalt?
-- EI esine:
select distinct t_lyhend_pc, t_lyhend_pn from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_pc is not null and t_lyhend_pn is not null;

-- palju ridu siis entryst kokku mapitakse?
-- kokku 23 488 rida, nüüd 26373 (67 022 kokku) mapitakse ära (kas pn või pc põhjal)
select * from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_pc is not null or t_lyhend_pn is not null;

-- mappimata jäänud read
select * from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_pc is null and t_lyhend_pn is null;
--B-Hc(Hematokritt) B-Plt(Trombotsüütide arv)P-ALP aluseline fosfataas

-- parandame mappingut ka LATEST_SUBSPECIAL CASES ABIL
-- see võtab arvesse ka UNITEID
--pn
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_pn = l."loinc_code(T-luhend)"
    from analysiscleaning.latest_subspecial_cases as l
    where lower(e.parameter_unit) = lower(l.unit) and
          lower(e.parameter_name) = lower(l.dirty_code);
--pc
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_pc = l."loinc_code(T-luhend)"
    from analysiscleaning.latest_subspecial_cases as l
    where lower(e.parameter_unit) = lower(l.unit) and
          lower(e.parameter_code_raw) = lower(l.dirty_code);
--an
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_an = l."loinc_code(T-luhend)"
    from analysiscleaning.latest_subspecial_cases as l
    where lower(e.parameter_unit) = lower(l.unit) and
          lower(e.analysis_name) = lower(l.dirty_code);

--mapitud ridu 24096, nüüd 27512
select parameter_name, parameter_code_raw from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_pc is not null or t_lyhend_pn is not null;

-- parandame mappingut võttes arvesse ka LATEST SUBSPECIAL_ANALYSISNAME CASES
-- ehk juhud kus T_lyhend sõötub ka analysis_names'st
--PN
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_pn = l."loinc_code(T-luhend)"
    from analysiscleaning.latest_supspecial_analysisname_cases as l
    where lower(e.analysis_name) = lower(l.analysis_name) and
          lower(e.parameter_name) = lower(l.dirty_code);
--PC
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_pc = l."loinc_code(T-luhend)"
    from analysiscleaning.latest_supspecial_analysisname_cases as l
    where lower(e.analysis_name) = lower(l.analysis_name) and
          lower(e.parameter_code_raw) = lower(l.dirty_code);
--AN
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_an = l."loinc_code(T-luhend)"
    from analysiscleaning.latest_supspecial_analysisname_cases as l
    where lower(e.analysis_name) = lower(l.analysis_name) and
          lower(e.analysis_name) = lower(l.dirty_code);

-- mappimata PC ja PN
select distinct parameter_name_clean, parameter_code_raw, array_agg(distinct (analysis_name, parameter_unit)), count(*)
from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_pc is null and t_lyhend_pn is null
group by parameter_name_clean, parameter_code_raw
order by count desc;

--mappimata PN
select parameter_name, array_agg(distinct parameter_name_clean), array_agg(distinct parameter_unit), count(*)
from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_pc is null and t_lyhend_pn is null and parameter_name is not null
group by parameter_name
order by count desc;

--mappimata PC
select parameter_code_raw, array_agg(distinct parameter_name_clean), array_agg(distinct parameter_unit), count(*)
from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_pc is null and t_lyhend_pn is null and parameter_name is not null
group by parameter_code_raw
order by count desc;



--- looma veeru mappinguks analysis_name järgi
alter table analysiscleaning.comparing_PC_PN_mappings_on_entry
    add column t_lyhend_an varchar;

update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_an = d."loinc_code(T-luhend)"
    from analysiscleaning.dirty_code_to_t_lyhed_mapping_all as d
    where lower(e.analysis_name) = lower(d.dirty_code);

--kasutan ka puhastatud dirty_code
update analysiscleaning.comparing_PC_PN_mappings_on_entry as e
set t_lyhend_an = d.t_lyhend
    from analysiscleaning.dirty_code_to_t_lyhed_mapping_all_clean as d
    where lower(e.analysis_name) = lower(d.clean_code)
          and e.t_lyhend_an is null;

--AN järgi saame mapitud 5744 rida
select * from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_an is not null;

---------------------------------------------------------------------------
--- kogu tabelist mapitud ridu 32956, nüüd 29 690, nüüd 31 288
select * from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_an is not null or t_lyhend_pn is not null or t_lyhend_pc is not null;


--- kogu tabelist mappimata ridu 38 335, nüüd 37 332, nüüd 35 732
-- mapitud 46%
select * from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_an is null and t_lyhend_pn is null and t_lyhend_pc is null

-- mappimata read
-- pn
--1070 rida
select parameter_name, array_agg(distinct parameter_unit), count(*) from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_an is null and t_lyhend_pn is null and t_lyhend_pc is null
group by parameter_name
order by count desc;


--pc
--1493 rida
select parameter_code_raw, count(*) from analysiscleaning.comparing_PC_PN_mappings_on_entry
where  t_lyhend_an is null and t_lyhend_pn is null and t_lyhend_pc is null
group by parameter_code_raw
order by count desc;

--an
--1141 rida
select analysis_name, array_agg(distinct parameter_unit), count(*) from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_an is null and t_lyhend_pn is null and t_lyhend_pc is null
group by analysis_name
order by count desc;










select * from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where dirty_code like '%globiin%'

select * from original.analysis
where epi_id = '46222812'

select * from original.analysis
where epi_id = '52213208'

select * from original_microset_fixed.analysis
where html like '%Referentsväärtus, otsustuspiir ja ühik%'

select * from original.analysis
where epi_id = '48919098'

select * from original.analysis
where epi_id = '52607639'

select * from work.run_ac_html_201902061644_analysis_html
where parse_type = '4.0'

--unknown table type
select html from original.analysis
where epi_id = '9992954' or epi_id = '11914488'

   ;or epi_id = '9679286' or epi_id = '9725647' or epi_id = '9753975' or
   epi_id = '9826391' or epi_id = '9899876' or epi_id = '9921168' or epi_id = '994750' or epi_id = '9970680' or
   epi_id = '9979960'

select * from original.analysis
where epi_id = '9263590'

select *  from original.analysis
where epi_id = '9257240'


select * from work.run_ac_html_201902061644_analysis_html
where epi_id = '52746443'


select * from original.analysis
where epi_id = '52746443'









--effective_time_raw from work.run_ac_html_201902061644_analysis_html
select * from work.runfull201902081139_analysis_html_new
where effective_time_raw like '%*%';


select * from work.run_ac_html_201901021226_analysis_html
where effective_time_raw is null

--viskame välja read, kus effective_time pole kuupäev
select
  case
    when effective_time_raw is Null then True -- =''
    when effective_time_raw ~ '^\d{2}.\d{2}.\d{4}$' then True
    when effective_time_raw ~ '^\d{4}.\d{2}.\d{2}$' then True
    when effective_time_raw ~ '^\d{2}.\d{2}.\d{4} \d{2}:\d{2}:\d{2}$' then True
    else False -- (delete from table work.run_ac_html_201901021226_analysis_html)_
  end as is_date, epi_id, effective_time_raw, analysis_name_raw, parameter_name_raw
from work.run_ac_html_201901021226_analysis_html -- work.runfull201902081139_analysis_html_new --
--where epi_id = '10140733'

--!!!!!!!!! TEHA TESTCASE NENDE KOHTA KUS VALES KOHAS ON KUUPÄEV JA MIDA EI SAA PARANDADA
select
  case
    when effective_time_raw is Null then True -- =''
    when effective_time_raw ~ '^\d{2}.\d{2}.\d{4}$' then True
    when effective_time_raw ~ '^\d{4}.\d{2}.\d{2}$' then True
    when effective_time_raw ~ '^\d{2}.\d{2}.\d{4} \d{2}:\d{2}:\d{2}$' then True
    else effective_time_raw = Null -- (delete from table work.run_ac_html_201901021226_analysis_html)_
  end as is_date, epi_id, effective_time_raw, analysis_name_raw, parameter_name_raw
from work.run_ac_html_201901021226_analysis_html;

--kõik mis kuupäeva kujul
select * from  work.run_ac_html_201901021226_analysis_html
--set effective_time_raw = Null
where not effective_time_raw ~ '^\d{2}.\d{2}.\d{4}$' and
      not effective_time_raw ~ '^\d{4}.\d{2}.\d{2}$' and
      not effective_time_raw ~ '^\d{2}.\d{2}.\d{4} \d{2}:\d{2}:\d{2}$';

select distinct effective_time_raw  from work.run_ac_html_201901021226_analysis_html;
select distinct effective_time_raw from work.runfull201902081139_analysis_html;

insert into original_microset_fixed.analysis (epi_id, epi_type, html)
    values ('000000000000', 'mingi_tyyp', '<table><thead><tr><th rowspan="2">Analüüs</th><th rowspan="2">Parameeter</th><th rowspan="2">Ühik</th><th rowspan="2">Referentsväärtus</th><th colspan="2">Tulemused</th></tr><tr><th>Kuupäev</th><th>Tulemus</th></tr></thead><tbody><tr><td>Hematoloogilised uuringud</td><td>Settereaktsioon(Optiline (Westergreni määramismeetod))</td><td>mm/h</td><td>&lt;20</td><td>05.10.2010</td><td>14</td></tr><tr><td>Hematoloogilised uuringud</td><td>Settereaktsioon(Optiline (Westergreni määramismeetod))</td><td>mm/h</td><td>&lt;20</td><td>04.10.2010</td><td>9</td></tr><tr><td>Hematoloogilised uuringud</td><td>B-HbA1C(Glükeeritud hemoglobiini fraktsioon A1c)</td><td>%</td><td>&lt;6,0</td><td>05.10.2010</td><td>6,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>WBC(Leukotsüüdid täisverest)</td><td>/nL</td><td>  3,8- 10,0</td><td>03.10.2010</td><td>9,4</td></tr><tr><td>Hematoloogilised uuringud</td><td>RBC(Erütrotsüüdid veres)</td><td>/pL</td><td> 4,50- 5,80</td><td>03.10.2010</td><td>3,76</td></tr><tr><td>Hematoloogilised uuringud</td><td>HGB(Hemoglobiin veres)</td><td>g/l</td><td>130 - 170</td><td>03.10.2010</td><td>115</td></tr><tr><td>Hematoloogilised uuringud</td><td>HCT(Hematokrit)</td><td>%</td><td>37 - 56</td><td>03.10.2010</td><td>34,4</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCV(Erütrotsüüdi keskmine maht)</td><td>fL</td><td>80 - 100</td><td>03.10.2010</td><td>91,5</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCH(Keskmine hemoglobiini hulk erütrotsüüdis)</td><td>pg</td><td>24 - 36</td><td>03.10.2010</td><td>30,6</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCHC(Keskmine hemoglobiini konsentratsioon erütrotsüüdis)</td><td>g/l</td><td>310 - 370</td><td>03.10.2010</td><td>334</td></tr><tr><td>Hematoloogilised uuringud</td><td>CHCM(Erütrotsüütide keskmine HGB kontsentratsioon (mahu kohta))</td><td>g/l</td><td>310 - 370</td><td>03.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>CH(Rakuline HGB (HGB keskmine sisaldus rakus))</td><td>pg</td><td>03.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>RDW(Erütrotsüütide suurusjaotuvuse (varjatsioonikoehvitsent))</td><td>%</td><td> 11,5- 16,0</td><td>03.10.2010</td><td>14,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>HDW(HGB kontsentratsiooni jaotuskõvera laius)</td><td>g/l</td><td>22 - 32</td><td>03.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>PLT(Trombotsüütide arv)</td><td>/nL</td><td>150 - 400</td><td>03.10.2010</td><td>182</td></tr><tr><td>Hematoloogilised uuringud</td><td>MPV(Trombotsüütide keskmine maht)</td><td>fL</td><td>5 - 10</td><td>03.10.2010</td><td>12,3</td></tr><tr><td>Hematoloogilised uuringud</td><td>PDW(Trombotsüütide jaotuvus mahujärgi. e. trombotsüütide anisotsütoosimäär)</td><td>%</td><td>12 - 20</td><td>03.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>PCT(Trombokrit)</td><td>%</td><td> 0,16- 0,35</td><td>03.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>%NEUT(Neutrofiilide  protsent, kepptuumsed ja segmenttuumsed.)</td><td>%</td><td>40 - 80</td><td>03.10.2010</td><td>63.0 (63)</td></tr><tr><td>Hematoloogilised uuringud</td><td>%LYMPH(Lümfotsüüdid protsent)</td><td>%</td><td> 20,0- 40,0</td><td>03.10.2010</td><td>26,2</td></tr><tr><td>Hematoloogilised uuringud</td><td>%MONO(Monotsüüdid protsent)</td><td>%</td><td>  2,0- 10,0</td><td>03.10.2010</td><td>5,6</td></tr><tr><td>Hematoloogilised uuringud</td><td>%EOS(Eosinofiilid protsent)</td><td>%</td><td>1 - 6</td><td>03.10.2010</td><td>4,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>%BASO(Basofiilid protsent)</td><td>%</td><td>  0,0-  2,0</td><td>03.10.2010</td><td>0,3</td></tr><tr><td>Hematoloogilised uuringud</td><td>%LUC(Suured mittevärvitavad rakud (Large unstai. calls) protsent)</td><td>%</td><td>&lt; 5</td><td>03.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>#NEUT(Neutrofiilid absoluut arv, kepptuumsed ja segmenttuumsed)</td><td>/nL</td><td>  1,9-  8,0</td><td>03.10.2010</td><td>5,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>#LYMPH(Lümfotsüüdid absoluutarv)</td><td>/nL</td><td>  0,9-  5,2</td><td>03.10.2010</td><td>2,5</td></tr><tr><td>Hematoloogilised uuringud</td><td>#MONO(Monotsüüdid absoluutarv)</td><td>/nL</td><td> 0,16- 1,00</td><td>03.10.2010</td><td>0,53</td></tr><tr><td>Hematoloogilised uuringud</td><td>#EOS(Eosinofiilid absoluutarv)</td><td>/nL</td><td>  0,0-  0,8</td><td>03.10.2010</td><td>0,5</td></tr><tr><td>Hematoloogilised uuringud</td><td>#BASO(Basofiilid absoluutarv)</td><td>/nL</td><td>0 - 0,2</td><td>03.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>#LUC(Suured mittevärvitavad rakud (Large unstai. calls))</td><td/><td>  0,0-  0,4</td><td>03.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>WBC(Leukotsüüdid täisverest)</td><td>/nL</td><td>  3,8- 10,0</td><td>05.10.2010</td><td>9,2</td></tr><tr><td>Hematoloogilised uuringud</td><td>RBC(Erütrotsüüdid veres)</td><td>/pL</td><td> 4,50- 5,80</td><td>05.10.2010</td><td>3,55</td></tr><tr><td>Hematoloogilised uuringud</td><td>HGB(Hemoglobiin veres)</td><td>g/l</td><td>130 - 170</td><td>05.10.2010</td><td>109</td></tr><tr><td>Hematoloogilised uuringud</td><td>HCT(Hematokrit)</td><td>%</td><td>37 - 56</td><td>05.10.2010</td><td>32,4</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCV(Erütrotsüüdi keskmine maht)</td><td>fL</td><td>80 - 100</td><td>05.10.2010</td><td>91,3</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCH(Keskmine hemoglobiini hulk erütrotsüüdis)</td><td>pg</td><td>24 - 36</td><td>05.10.2010</td><td>30,7</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCHC(Keskmine hemoglobiini konsentratsioon erütrotsüüdis)</td><td>g/l</td><td>310 - 370</td><td>05.10.2010</td><td>336</td></tr><tr><td>Hematoloogilised uuringud</td><td>CHCM(Erütrotsüütide keskmine HGB kontsentratsioon (mahu kohta))</td><td>g/l</td><td>310 - 370</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>CH(Rakuline HGB (HGB keskmine sisaldus rakus))</td><td>pg</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>RDW(Erütrotsüütide suurusjaotuvuse (varjatsioonikoehvitsent))</td><td>%</td><td> 11,5- 16,0</td><td>05.10.2010</td><td>14,4</td></tr><tr><td>Hematoloogilised uuringud</td><td>HDW(HGB kontsentratsiooni jaotuskõvera laius)</td><td>g/l</td><td>22 - 32</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>PLT(Trombotsüütide arv)</td><td>/nL</td><td>150 - 400</td><td>05.10.2010</td><td>153</td></tr><tr><td>Hematoloogilised uuringud</td><td>MPV(Trombotsüütide keskmine maht)</td><td>fL</td><td>5 - 10</td><td>05.10.2010</td><td>12,1</td></tr><tr><td>Hematoloogilised uuringud</td><td>PDW(Trombotsüütide jaotuvus mahujärgi. e. trombotsüütide anisotsütoosimäär)</td><td>%</td><td>12 - 20</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>PCT(Trombokrit)</td><td>%</td><td> 0,16- 0,35</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>%NEUT(Neutrofiilide  protsent, kepptuumsed ja segmenttuumsed.)</td><td>%</td><td>40 - 80</td><td>05.10.2010</td><td>58,8</td></tr><tr><td>Hematoloogilised uuringud</td><td>%LYMPH(Lümfotsüüdid protsent)</td><td>%</td><td> 20,0- 40,0</td><td>05.10.2010</td><td>26,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>%MONO(Monotsüüdid protsent)</td><td>%</td><td>  2,0- 10,0</td><td>05.10.2010</td><td>6,2</td></tr><tr><td>Hematoloogilised uuringud</td><td>%EOS(Eosinofiilid protsent)</td><td>%</td><td>1 - 6</td><td>05.10.2010</td><td>7,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>%BASO(Basofiilid protsent)</td><td>%</td><td>  0,0-  2,0</td><td>05.10.2010</td><td>0,2</td></tr><tr><td>Hematoloogilised uuringud</td><td>%LUC(Suured mittevärvitavad rakud (Large unstai. calls) protsent)</td><td>%</td><td>&lt; 5</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>#NEUT(Neutrofiilid absoluut arv, kepptuumsed ja segmenttuumsed)</td><td>/nL</td><td>  1,9-  8,0</td><td>05.10.2010</td><td>5,4</td></tr><tr><td>Hematoloogilised uuringud</td><td>#LYMPH(Lümfotsüüdid absoluutarv)</td><td>/nL</td><td>  0,9-  5,2</td><td>05.10.2010</td><td>2,5</td></tr><tr><td>Hematoloogilised uuringud</td><td>#MONO(Monotsüüdid absoluutarv)</td><td>/nL</td><td> 0,16- 1,00</td><td>05.10.2010</td><td>0,57</td></tr><tr><td>Hematoloogilised uuringud</td><td>#EOS(Eosinofiilid absoluutarv)</td><td>/nL</td><td>  0,0-  0,8</td><td>05.10.2010</td><td>0,7</td></tr><tr><td>Hematoloogilised uuringud</td><td>#BASO(Basofiilid absoluutarv)</td><td>/nL</td><td>0 - 0,2</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>#LUC(Suured mittevärvitavad rakud (Large unstai. calls))</td><td/><td>  0,0-  0,4</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>WBC(Leukotsüüdid täisverest)</td><td>/nL</td><td>  3,8- 10,0</td><td>04.10.2010</td><td>9,4</td></tr><tr><td>Hematoloogilised uuringud</td><td>RBC(Erütrotsüüdid veres)</td><td>/pL</td><td> 4,50- 5,80</td><td>04.10.2010</td><td>3,42</td></tr><tr><td>Hematoloogilised uuringud</td><td>HGB(Hemoglobiin veres)</td><td>g/l</td><td>130 - 170</td><td>04.10.2010</td><td>104</td></tr><tr><td>Hematoloogilised uuringud</td><td>HCT(Hematokrit)</td><td>%</td><td>37 - 56</td><td>04.10.2010</td><td>30,8</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCV(Erütrotsüüdi keskmine maht)</td><td>fL</td><td>80 - 100</td><td>04.10.2010</td><td>90,1</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCH(Keskmine hemoglobiini hulk erütrotsüüdis)</td><td>pg</td><td>24 - 36</td><td>04.10.2010</td><td>30,4</td></tr><tr><td>Hematoloogilised uuringud</td><td>MCHC(Keskmine hemoglobiini konsentratsioon erütrotsüüdis)</td><td>g/l</td><td>310 - 370</td><td>04.10.2010</td><td>338</td></tr><tr><td>Hematoloogilised uuringud</td><td>CHCM(Erütrotsüütide keskmine HGB kontsentratsioon (mahu kohta))</td><td>g/l</td><td>310 - 370</td><td>04.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>CH(Rakuline HGB (HGB keskmine sisaldus rakus))</td><td>pg</td><td>04.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>RDW(Erütrotsüütide suurusjaotuvuse (varjatsioonikoehvitsent))</td><td>%</td><td> 11,5- 16,0</td><td>04.10.2010</td><td>14,5</td></tr><tr><td>Hematoloogilised uuringud</td><td>HDW(HGB kontsentratsiooni jaotuskõvera laius)</td><td>g/l</td><td>22 - 32</td><td>04.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>PLT(Trombotsüütide arv)</td><td>/nL</td><td>150 - 400</td><td>04.10.2010</td><td>145</td></tr><tr><td>Hematoloogilised uuringud</td><td>MPV(Trombotsüütide keskmine maht)</td><td>fL</td><td>5 - 10</td><td>04.10.2010</td><td>11,4</td></tr><tr><td>Hematoloogilised uuringud</td><td>PDW(Trombotsüütide jaotuvus mahujärgi. e. trombotsüütide anisotsütoosimäär)</td><td>%</td><td>12 - 20</td><td>04.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>PCT(Trombokrit)</td><td>%</td><td> 0,16- 0,35</td><td>04.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>%NEUT(Neutrofiilide  protsent, kepptuumsed ja segmenttuumsed.)</td><td>%</td><td>40 - 80</td><td>04.10.2010</td><td>68,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>%LYMPH(Lümfotsüüdid protsent)</td><td>%</td><td> 20,0- 40,0</td><td>04.10.2010</td><td>21,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>%MONO(Monotsüüdid protsent)</td><td>%</td><td>  2,0- 10,0</td><td>04.10.2010</td><td>5,9</td></tr><tr><td>Hematoloogilised uuringud</td><td>%EOS(Eosinofiilid protsent)</td><td>%</td><td>1 - 6</td><td>04.10.2010</td><td>3,1</td></tr><tr><td>Hematoloogilised uuringud</td><td>%BASO(Basofiilid protsent)</td><td>%</td><td>  0,0-  2,0</td><td>04.10.2010</td><td>0,2</td></tr><tr><td>Hematoloogilised uuringud</td><td>%LUC(Suured mittevärvitavad rakud (Large unstai. calls) protsent)</td><td>%</td><td>&lt; 5</td><td>04.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>#NEUT(Neutrofiilid absoluut arv, kepptuumsed ja segmenttuumsed)</td><td>/nL</td><td>  1,9-  8,0</td><td>04.10.2010</td><td>6,5</td></tr><tr><td>Hematoloogilised uuringud</td><td>#LYMPH(Lümfotsüüdid absoluutarv)</td><td>/nL</td><td>  0,9-  5,2</td><td>04.10.2010</td><td>2,1</td></tr><tr><td>Hematoloogilised uuringud</td><td>#MONO(Monotsüüdid absoluutarv)</td><td>/nL</td><td> 0,16- 1,00</td><td>04.10.2010</td><td>0,55</td></tr><tr><td>Hematoloogilised uuringud</td><td>#EOS(Eosinofiilid absoluutarv)</td><td>/nL</td><td>  0,0-  0,8</td><td>04.10.2010</td><td>0,3</td></tr><tr><td>Hematoloogilised uuringud</td><td>#BASO(Basofiilid absoluutarv)</td><td>/nL</td><td>0 - 0,2</td><td>04.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>#LUC(Suured mittevärvitavad rakud (Large unstai. calls))</td><td/><td>  0,0-  0,4</td><td>04.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>Kepptuum. neut.</td><td>%</td><td>1 - 5</td><td>05.10.2010</td><td>2,6</td></tr><tr><td>Hematoloogilised uuringud</td><td>Segmenttuumsed</td><td>%</td><td>50 - 65</td><td>05.10.2010</td><td>62,8</td></tr><tr><td>Hematoloogilised uuringud</td><td>Eosinofiilid</td><td>%</td><td>1 - 4</td><td>05.10.2010</td><td>6,8</td></tr><tr><td>Hematoloogilised uuringud</td><td>Basofiilid</td><td>%</td><td>0 - 1</td><td>05.10.2010</td><td>0,5</td></tr><tr><td>Hematoloogilised uuringud</td><td>Lümfotsüüdid</td><td>%</td><td>20 - 40</td><td>05.10.2010</td><td>24,6</td></tr><tr><td>Hematoloogilised uuringud</td><td>Monotsüüdid</td><td>%</td><td>6 - 8</td><td>05.10.2010</td><td>2,6</td></tr><tr><td>Hematoloogilised uuringud</td><td>Suured teralised lümfotsüüdid</td><td>%</td><td>05.10.2010</td><td>*</td></tr><tr><td>Hematoloogilised uuringud</td><td>Er. anisotsütoos</td><td>korda</td><td>05.10.2010</td><td>1</td></tr><tr><td>Hematoloogilised uuringud</td><td>Er.poikilotsütoos</td><td>korda</td><td>05.10.2010</td><td>0</td></tr><tr><td>Hematoloogilised uuringud</td><td>Er.mikrotsütoos</td><td>korda</td><td>05.10.2010</td><td>0</td></tr><tr><td>Hematoloogilised uuringud</td><td>Er.makrotsütoos</td><td>korda</td><td>05.10.2010</td><td>1</td></tr><tr><td>Hematoloogilised uuringud</td><td>Er. polükroomsus</td><td>korda</td><td>05.10.2010</td><td>0</td></tr><tr><td>Hematoloogilised uuringud</td><td>Er. hüpokroomsus</td><td>korda</td><td>05.10.2010</td><td>0</td></tr><tr><td>Hüübimissüsteemi uuringud</td><td>P-APTT(Aktiveeritud partsiaalse tromboplastiini aeg)</td><td>s</td><td>26 - 38</td><td>03.10.2010</td><td>41,6</td></tr><tr><td>Hüübimissüsteemi uuringud</td><td>P-PT-INR(Protrombiini aeg; INR)</td><td>suhtarv</td><td> 0,90- 1,18</td><td>03.10.2010</td><td>1,11</td></tr><tr><td>Hüübimissüsteemi uuringud</td><td>P-PT-INR(Protrombiini aeg; INR)</td><td>suhtarv</td><td> 0,90- 1,18</td><td>06.10.2010</td><td>1,16</td></tr><tr><td>Hüübimissüsteemi uuringud</td><td>P-PT-INR(Protrombiini aeg; INR)</td><td>suhtarv</td><td> 0,90- 1,18</td><td>07.10.2010</td><td>1,41</td></tr><tr><td>Immuunhematoloogia</td><td>ABO ja Rh(D) esmane(ABO-veregrupi ja Rh(D) kinnitav määramine (ABO-grupp määratud nii otsese kui ka pöördreaktsiooniga) TEOSTATATAKSE LABORIS)</td><td/><td>03.10.2010</td><td>O RhD positiivne</td></tr><tr><td>Immuunhematoloogia</td><td>ABO esmane osakonnas(ABO-veregrupi määramine patsiendi identifitseerimisel või erütrokomponentide kontrollil TEOSTATAKSE OSAKONNAS VÕI PROTSEDUURITOAS)</td><td/><td>03.10.2010</td><td>O</td></tr><tr><td>Immuunhematoloogia</td><td>AK(Erütrotsütaarsete antikehade sõeluuring kahe erütrotsüüdiga)</td><td/><td>03.10.2010</td><td>Negatiivne</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-ALAT(Alaniini aminotransferaas)</td><td>U/l</td><td>&lt; 55</td><td>03.10.2010</td><td>26</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-ASAT(Aspartaadi aminotransferaas)</td><td>U/l</td><td>5 - 34</td><td>03.10.2010</td><td>28</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Urea(Uurea)</td><td>mmol/l</td><td> 3,00- 9,20</td><td>03.10.2010</td><td>8,80</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Urea(Uurea)</td><td>mmol/l</td><td> 3,00- 9,20</td><td>05.10.2010</td><td>5,70</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Urea(Uurea)</td><td>mmol/l</td><td> 3,00- 9,20</td><td>04.10.2010</td><td>5,60</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Crea(Kreatiniin)</td><td>µmol/l</td><td>03.10.2010</td><td>92.0 (92)</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Crea(Kreatiniin)</td><td>µmol/l</td><td>05.10.2010</td><td>80</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Crea(Kreatiniin)</td><td>µmol/l</td><td>04.10.2010</td><td>71</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-Chol(Kolesterool)</td><td>mmol/l</td><td>&lt;5,70</td><td>04.10.2010</td><td>3.90 (3,90)</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Trigl(Triglütseriidid)</td><td>mmol/l</td><td> 0,10- 2,20</td><td>04.10.2010</td><td>0,92</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-HDL-Chol(HDL-Kolesterool)</td><td>mmol/l</td><td>&gt;1,00</td><td>04.10.2010</td><td>1,06</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-LDL-Chol(LDL-Kolesterool)</td><td>mmol/l</td><td>&lt;3,50</td><td>04.10.2010</td><td>2,29</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-CRP(C-reaktiivne valk)</td><td>mg/l</td><td>&lt;5,0</td><td>03.10.2010</td><td>3,2</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-CRP(C-reaktiivne valk)</td><td>mg/l</td><td>&lt;5,0</td><td>05.10.2010</td><td>26,5</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-CRP(C-reaktiivne valk)</td><td>mg/l</td><td>&lt;5,0</td><td>04.10.2010</td><td>15,7</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Gluc(Glükoos seerumis)</td><td>mmol/l</td><td>3,9 - 6</td><td>03.10.2010</td><td>8,71</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Gluc(Glükoos seerumis)</td><td>mmol/l</td><td>3,9 - 6</td><td>05.10.2010</td><td>6,72</td></tr><tr><td>Kliinilise keemia uuringud</td><td>fS-Gluc(Glükoos seerumis)</td><td>mmol/l</td><td>3,9 - 6</td><td>04.10.2010</td><td>5,47</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-Na(Naatrium)</td><td>mmol/l</td><td>136 - 145</td><td>03.10.2010</td><td>143</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-Na(Naatrium)</td><td>mmol/l</td><td>136 - 145</td><td>05.10.2010</td><td>141</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-Na(Naatrium)</td><td>mmol/l</td><td>136 - 145</td><td>04.10.2010</td><td>139</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-K(Kaalium)</td><td>mmol/l</td><td>3,5 - 5,1</td><td>03.10.2010</td><td>4,4</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-K(Kaalium)</td><td>mmol/l</td><td>3,5 - 5,1</td><td>05.10.2010</td><td>4,2</td></tr><tr><td>Kliinilise keemia uuringud</td><td>S-K(Kaalium)</td><td>mmol/l</td><td>3,5 - 5,1</td><td>04.10.2010</td><td>4,1</td></tr><tr><td>Uriini uuringud</td><td>U-SG(Uriini erikaal)</td><td>g/cm³</td><td>1,015-1,025</td><td>03.10.2010</td><td>1.020 (1,020)</td></tr><tr><td>Uriini uuringud</td><td>U-pH(Uriini pH)</td><td>pH</td><td>4,5 - 8</td><td>03.10.2010</td><td>5</td></tr><tr><td>Uriini uuringud</td><td>U-Leu(Leukotsüüdid uriinis)</td><td>µl</td><td>neg</td><td>03.10.2010</td><td>neg</td></tr><tr><td>Uriini uuringud</td><td>U-Nit(Nitritid uriinis)</td><td/><td>neg</td><td>03.10.2010</td><td>neg</td></tr><tr><td>Uriini uuringud</td><td>U-Pro(Valk uriinis)</td><td>g/l</td><td>neg</td><td>03.10.2010</td><td>neg</td></tr><tr><td>Uriini uuringud</td><td>U-Glu(Glükoos uriinis)</td><td>mmol/l</td><td>norm</td><td>03.10.2010</td><td>17 mmol/l, 3+</td></tr><tr><td>Uriini uuringud</td><td>U-Ket(Ketoonid uriinis)</td><td>mmol/l</td><td>neg</td><td>03.10.2010</td><td>neg</td></tr><tr><td>Uriini uuringud</td><td>U-UBG(Urobilinogeen)</td><td>µmol/l</td><td>norm</td><td>03.10.2010</td><td>norm</td></tr><tr><td>Uriini uuringud</td><td>U-Bil(Bilirubiin uriinis)</td><td>µmol/l</td><td>neg</td><td>03.10.2010</td><td>neg</td></tr><tr><td>Uriini uuringud</td><td>U-Ery(Erütrotsüüdid uriinis)</td><td>µl</td><td>neg</td><td>03.10.2010</td><td>10 /ul, 1+</td></tr></tbody></table>');

select *
from work.runfull201902081139_analysis_html_new
where epi_id = '10140733';

select * from original.analysis
where epi_id = '10140733'

select *
from work.run_ac_html_201901171409_analysis_html
where effective_time_raw is Null --or effective_time_raw = '';

select *
from work.run_ac_html_201902061644_analysis_html
where epi_id = '53015838' and
      (--analysis_name_raw like '%a1178 Hemogramm 5-osalise leukogrammiga%' or
        parameter_name_raw like '%a2034 WBC%' or
        parameter_name_raw like '%a2035 RBC%' or
        parameter_name_raw like '%a2041 Hb%' or
        parameter_name_raw like '%a2042 Hct%');

select *
from original_microset_fixed.analysis
where epi_id = '52746443';

select * from work.run_ac_html_201902061644_analysis_html
where epi_id = '52746443' --and analysis_name_raw = 'Hemogramm 3 osalise leukogrammiga'

select * from work.runfull201902081139_analysis_html
WHERE not effective_time_raw ~ '^\d{2}.\d{2}.\d{4}$' and
     not effective_time_raw ~ '^\d{4}.\d{2}.\d{2}$' and
     not effective_time_raw ~ '^\d{2}.\d{2}.\d{4} \d{2}:\d{2}:\d{2}$' and
     not effective_time_raw ~ '^\d{2}.\d{2}.\d{4} \d{2}:\d{2}$' and -- TESTI SEDA
     not effective_time_raw  = '';

create table analysiscleaning.xxxkatse (
  id  varchar,
  tim  varchar
);

insert into analysiscleaning.xxxkatse (id, tim)
values ('0000', '01.02.2018 12:32');

ALTER TABLE analysiscleaning.xxxkatse
                         ALTER COLUMN tim TYPE TIMESTAMP
                         USING tim::timestamp without time zone

select * from original.analysis
where epi_id = '10140733'; --mitte toimiv 10140733
--toimiv 2.3 epi_id = '10024327'--
--epi_id = '10212755' siin sama viga mis mitte toimival aga parandatakse parsimisel ära, ainus selline, mis on orginal_microsetis.analysis
-- sama epi_id, need ainult originalis, parandatakse viga ära
-- 8559518
-- 24265784
-- 8065194
-- 10212755

select  * from work.runfull201902081139_analysis_html
where epi_id = '10140733' and (parameter_name_raw like 'Settereaktsioon%' or parameter_name_raw like 'CH(Rakuli%'
                                 or parameter_name_raw like 'RDW(Er%');
-- epi_id = '10140733'

select *
from work.runfull201902081139_analysis_html_log
where message like '%ess%';

select distinct message from work.runfull201902081139_analysis_html_log
where type = 'error:'