-- Yhendlabori fail on classifications repos asukohaga loinc/elhr-digilugu-ee-algandmed/elabor_algandmed.csv
-- Empiiriliseld määran, mis veerud vastavas analysis_name'le (AN), parameter_name'le (PN), parameter_code'le (PC), unit'le (U)
-- AN -> t_nimetus
-- PN ->  kuna AN  on 't_nimetus' ja PC on 't_lyhend', siis jätan PN vastavusse 'kasutatav_nimetus'
-- PC -> t_lyhend
-- U -> t_yhik
-- täpsem selgitus t_nimetuse, _t_lyhendi ja kasutatava_nimetuse kohta classifications/loinc/elhr-digilugu-ee-algandmed/docs
-- Analuuside-nimetuste-moodustamise-poh


-- t_nimetus ja t_lyhend on täpsemad kasutatavast nimetusest
-- valdkond,t_lyhend,kasutatav_nimetus,t_nimetus
-- valdkond, t_lyhend,         kasutatav_nimetus, t_nimetus
-- CSF,      CSF-Basofiilid %, Basofiilid %,      Basofiilide suhtarv liikvoris
-- BoF,      PrtF-Basofiilid %,Basofiilid %,      Basofiilide suhtarv kõhuõõnevedelikus


select * from classifications.elabor_analysis
where kasutatav_nimetus like 'Baso%'
select * from classifications.elabor_analysis
where t_nimetus like 'Baso%'
select * from classifications.elabor_analysis
where t_lyhend like '%Baso%'

select * from work.run_ac_html_201901171459_analysis_html
where parameter_name_raw like 'Basofiil%'
select * from work.run_ac_html_201901171459_analysis_html
where parameter_name_raw like 'Baso#'

select * from analysiscleaning.long_loinc_mapping
where parameter_name like 'Basofiil%'
select * from analysiscleaning.long_loinc_mapping
where parameter_name like 'Baso%'

select distinct parameter_name_raw, count(*) from work.run_ac_html_201901171459_analysis_html
where analysis_name_raw like 'Hemogramm%'
group by parameter_name_raw;

--B-CBC-5Diff
select distinct parameter_code, count(*) from analysiscleaning.long_loinc_mapping
group by parameter_code
order by count desc;

select * from classifications.elabor_analysis
where t_lyhend like 'B-CBC%'; -- 'B-CBC-5Diff%'

-- vajalik yhendlabori tabel, 4676 rida
select t_nimetus, kasutatav_nimetus, t_lyhend, t_yhik, loinc_no from classifications.elabor_analysis;
-- long_loinc_mappingus 12 000 rida

select analysis_name, t_nimetus, parameter_name, kasutatav_nimetus, parameter_code, t_lyhend, unit,t_yhik, loinc_no
from analysiscleaning.long_loinc_mapping as l
left join classifications.elabor_analysis as c
    on --c.t_nimetus = l.analysis_name and
       c.t_lyhend = l.parameter_code
       --c.kasutatav_nimetus = l.parameter_name
       --(c.kasutatav_nimetus = l.parameter_name or c.t_nimetus = l.parameter_name or c.t_lyhend = l.parameter_name)
       -- c.t_yhik = l.unit
--where t_nimetus is not null;


-- võtame ühe populaarse paneeli, selle abil leiam vastavused
-- populaarseim parameetri nimi on RBC, teine MCV
select parameter_name, count(*) from analysiscleaning.long_loinc_mapping
group by parameter_name
order by count desc

--populaarseim parameter_code on B-CBC+Diff, teine U-Sedim
select parameter_code, count(*) from analysiscleaning.long_loinc_mapping
group by parameter_code
order by count desc

--populaarseim analysis_name on Mikrobioloogilised uuringud, Hematoloogilised uuringud
select analysis_name, count(*) from analysiscleaning.long_loinc_mapping
group by analysis_name
order by count desc



--otsime ühendlaborist vastavusi top 1
-- parameter_namele vastab kasutatav niemtus
select * from classifications.elabor_analysis
where kasutatav_nimetus = 'RBC'

-- populaarseimale parameter_code-le ei leidu vastavsut ühendlabpirs!!!!
select * from classifications.elabor_analysis
where t_lyhend = 'B-CBC+Diff' or kasutatav_nimetus = 'B-CBC+Diff' or t_nimetus = 'B-CBC+Diff'

-- populaarseimale parameter_code-le ei leidu vastavsut ühendlabpirs!!!
select * from classifications.elabor_analysis
where t_lyhend = 'Mikrobioloogilised uuringud' or kasutatav_nimetus = 'Mikrobioloogilised uuringud'
    or t_nimetus = 'Mikrobioloogilised uuringud'



--otsime ühendlaborist vastusi populaarsuselt teisele

select * from classifications.elabor_analysis
where kasutatav_nimetus = 'MCV'

-- parameter_code-le ei leidu vastavsut ühendlaboris!!!!
select * from classifications.elabor_analysis
where t_lyhend = 'U-Sedim' or kasutatav_nimetus = 'U-Sedim' or t_nimetus = 'U-Sedim'

-- populaarseimale analysis_namele ei leidu vastavsut ühendlaboris!!!
select * from classifications.elabor_analysis
where t_lyhend = 'Hematoloogilised uuringud' or kasutatav_nimetus = 'Hematoloogilised uuringud'
    or t_nimetus = 'Hematoloogilised uuringud'

--kas mõnes  veerus üldse leidub see väärtus?  ei leidu...
select * from classifications.elabor_analysis
where t_lyhend = 'Hematoloogilised uuringud' or kasutatav_nimetus = 'Hematoloogilised uuringud'
    or t_nimetus = 'Hematoloogilised uuringud' or  component =  'Hematoloogilised uuringud'
    or property = 'Hematoloogilised uuringud' or system = 'Hematoloogilised uuringud'
    or long_common_name = 'Hematoloogilised uuringud' or short_name = 'Hematoloogilised uuringud'

select * from classifications.elabor_analysis
where t_lyhend  like '%Hematoloog%' or kasutatav_nimetus like '%Hematoloog%'
    or t_nimetus like '%Hematoloog%';


select *
from classifications.elabor_analysis
where system = 'Bld';