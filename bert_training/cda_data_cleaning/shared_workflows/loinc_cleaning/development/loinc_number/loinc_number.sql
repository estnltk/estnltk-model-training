--------------------------------------------------------
-- example of problem:

-- in long_loinc_mapping analyte is short name for example "Mono", we need long name "Monocyte"
-- get  the corresponding long names from loinc_analytes (in classification repo)
select la.analyte_name_en as component, ll.* from analysiscleaning.long_loinc_mapping as ll
                  left join analysiscleaning.loinc_analytes as la
                  on ll.analyte = la.analyte
--where analysis_name = 'Hemogramm viieosalise leukogrammiga' and parameter_name = 'Mono%' and parameter_code = 'B-CBC-5Diff'

-- 3 possible loinc codes for "Monocytes" in tablecore
-- to know which one is right we need method_typ but we dont know it
select * from analysiscleaning.loinctablecore
where component like 'Monocytes' and property = 'NCnc' and system like 'Bld'
order by component asc;

select distinct method_typ, count(*) from analysiscleaning.loinctablecore
group by method_typ
order by count desc ;

----------------------------------------------------------------------------
-- best possible loinc number mapping

-- we  leave in table every loinc mapping (component, property, system, time_aspect, scale_type) with (multiple) possible loinc_number matches
select  ll.component,
        ll.analyte,
        ll.property,
        ll.system,
        ll.time_aspect,
        ll.scale,
        array_agg(distinct tc.loinc_num) as loinc_number_array,
        array_agg(distinct tc.method_typ) as method_typ_array --distinct tc.loinc_num, ll.analyte, tc.component, tc.property, tc.system, tc.time_aspct, tc.scale_typ, tc.method_typ, ll.analysis_name, ll.parameter_name, ll.parameter_code, ll.unit
from
        (-- long_loinc_mapping containing long english analyte/component name column
         select la.analyte_name_en as component, llm.* from analysiscleaning.long_loinc_mapping as llm
         left join analysiscleaning.loinc_analytes as la
         on llm.analyte = la.analyte
         )
         as ll
  left join analysiscleaning.loinctablecore as tc
  on  ll.property = tc.property and
      ll.system = tc.system and
      ll.time_aspect = tc.time_aspct and
      ll.scale = tc.scale_typ and
      tc.component ILIKE '%' || ll.component || '%'
  --empty rows are not interesing
  where (ll.component is not null and ll.property is not null and ll.time_aspect is not null and ll.scale is not null)
group by ll.component, ll.analyte, ll.property, ll.system, ll.time_aspect, ll.scale;

--as we dont have method_typ, (analyte, propery, system, time_aspect, scale) can have multiple loinc numbers

-- 76 rows without any loinc numbers
-- 43 rows with at least one loinc number



------------------------------------------------------------------------------------------------
-- example of multiple loincs:

-- mapping analyte,property,system,time_aspect,scale
-- HGB,MCnc,Bld,Pt,Qn
select la.analyte_name_en as component, ll.* from analysiscleaning.long_loinc_mapping as ll
                  left join analysiscleaning.loinc_analytes as la
                  on ll.analyte = la.analyte
where analysis_name = 'Hematoloogilised uuringud' and
      parameter_name = 'B-Hb(Hemoglobiini kontsentratsioon)' and
      parameter_code = '158';

-- has  4 different loinc codes
select * from analysiscleaning.loinctablecore
where component = 'Hemoglobin' and
  property = 'MCnc' and
  system = 'Bld' and
  time_aspct = 'Pt' and
  scale_typ = 'Qn'


select distinct component, * from analysiscleaning.loinctablecore
where component like '%Alanine aminotransferase%' and system like '%Ser%' and property = 'CCnc'
order by component asc;


select distinct component, loinc_num, system, property, time_aspct, scale_typ from analysiscleaning.loinctablecore
where component like '%Iron%' and system like '%Ser%' and property = 'SCnc'
order by component asc;