select * from analysiscleaning.elabor_parameter_name_to_loinc_mapping;
select * from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping;
select * from analysiscleaning.parameter_name_to_loinc_mapping;
select * from analysiscleaning.parameter_name_parameter_unit_to_loinc_mapping;

select * from
  (select parameter_name, array_agg(distinct loinc_code) as loincs
   from analysiscleaning.elabor_parameter_name_to_loinc_mapping
    group by parameter_name) as e
where array_length(e.loincs, 1) > 1;
-- pole ühtegi mitesust

select * from
  (select parameter_name, t_yhik, array_agg(distinct loinc_code) as loincs
   from analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping
    group by parameter_name, t_yhik) as e
where array_length(e.loincs, 1) > 1;
-- pole ühtegi mitesust


select * from
  (
    select parameter_name, array_agg(distinct (t_lyhend)), array_agg(distinct evidence), array_agg(distinct loinc_code) as loincs
    from analysiscleaning.parameter_name_to_loinc_mapping
    group by parameter_name
  ) as e
where array_length(e.loincs, 1) > 1;
-- pole enam mitmesusi


select * from
  (
    select parameter_name, parameter_unit, array_agg(distinct loinc_code) as loincs, array_agg(distinct evidence)
    from analysiscleaning.predefined_csvs_parameter_name_parameter_unit_to_loinc_mapping
    group by parameter_name, parameter_unit
  ) as e
where array_length(e.loincs, 1) > 1;
-- 1 mitmesus!










----------------------------------------------------------------------------------------------------------------------
-- MAPPINGU ÜHESEKS TEGEMINE
----------------------------------------------------------------------------------------------------------------------
-- T_NIMETUSE mitmesused
----------------------------------------------------------------------------------------------------------------------
-- leida sellised t_nimetused, kus ühele t_nimetusele vastab mitu erinevat t_lyhendit


--!!!!!!!!!!!!!!!!!!!! Veider!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
select t_lyhend, t_nimetus, kasutatav_nimetus, loinc_no, t_yhik from classifications.elabor_analysis
where t_lyhend = 'B-Eo#' or t_lyhend = 'B-Eosinofiilid #'

-- t_lyh            t_nim             kas_nim       loinc     yhik
--B-Eo#            Eosinofiilide arv   Eo           711-2     E9/L
--B-Eosinofiilid # Eosinofiilide arv   Eosinofiilid 26449-9   E9/L
    -- https://r.details.loinc.org/LOINC/711-2.html?sections=Comprehensive
    -- https://s.details.loinc.org/LOINC/26449-9.html?sections=Comprehensive
-- erinevus tuleb sisse sellest, et esimesel on meetod määratud ja teisel mitte!!



-- need on ÜHESED mappingud
--4585
select * from (
                select t_nimetus, array_agg(distinct t_lyhend) as t_lyhendid
                from classifications.elabor_analysis
                group by t_nimetus
              ) as c
where array_length(c.t_lyhendid, 1) = 1;

-- need on mitmesed mappingud
-- 41
select * from (
                select t_nimetus, array_agg(distinct t_lyhend) as t_lyhendid
                from classifications.elabor_analysis
                group by t_nimetus
              ) as c
where array_length(c.t_lyhendid, 1) > 1;


-- äkki saab mitmesed mappingud koos UNITIGA üheseks
-- MITMESED mappingud ka koos unitiga 25
-- seega peaks uniti lisamine ära mappima 41-25 = 16

-- T_NIMETUS + UNIT MAPPING
--41
select * from (
                select t_nimetus, t_yhik, array_agg(distinct t_lyhend) as t_lyhendid
                from classifications.elabor_analysis
                group by t_nimetus, t_yhik
              ) as c
where array_length(c.t_lyhendid, 1) = 1 and
      t_nimetus not in (
        -- ainult t_nimetuse järgi üheselt mappitud
              select c.t_nimetus from (
                  select t_nimetus, array_agg(distinct t_lyhend) as t_lyhendid
                  from classifications.elabor_analysis
                  group by t_nimetus
              ) as c
              where array_length(c.t_lyhendid, 1) = 1
    )
;

-- Täielikult unmapped
-- 25
select * from (
                select t_nimetus, t_yhik, array_agg(distinct t_lyhend) as t_lyhendid
                from classifications.elabor_analysis
                group by t_nimetus, t_yhik
              ) as c
where array_length(c.t_lyhendid, 1) > 1;





----------------------------------------------------------------------------------------------------------------------
--KASUTATAVA_NIMETUSE mitmesused
----------------------------------------------------------------------------------------------------------------------
-- need on ÜHESED mappingud
--4186
select * from (
                select kasutatav_nimetus, array_agg(distinct t_lyhend) as t_lyhendid
                from classifications.elabor_analysis
                group by kasutatav_nimetus
              ) as c
where array_length(c.t_lyhendid, 1) = 1;


-- need on mitmesed mappingud
-- 164
select * from (
                select kasutatav_nimetus, array_agg(distinct t_lyhend) as t_lyhendid
                from classifications.elabor_analysis
                group by kasutatav_nimetus
              ) as c
where array_length(c.t_lyhendid, 1) > 1;



-- KASUTATAV_NIMETUS + UNIT MAPPING
-- 61 rida
select * from (
                select kasutatav_nimetus, t_yhik, array_agg(distinct t_lyhend) as t_lyhendid
                from classifications.elabor_analysis
                group by kasutatav_nimetus, t_yhik
              ) as c
where array_length(c.t_lyhendid, 1) = 1 and
      kasutatav_nimetus not in (
        -- ainult t_nimetuse järgi juba üheselt mappitud
              select c.kasutatav_nimetus from (
                  select kasutatav_nimetus, array_agg(distinct t_lyhend) as t_lyhendid
                  from classifications.elabor_analysis
                  group by kasutatav_nimetus
              ) as c
              where array_length(c.t_lyhendid, 1) = 1
    )
;



-- Täielikult unmapped
-- 146
select * from (
                select kasutatav_nimetus, t_yhik, array_agg(distinct t_lyhend) as t_lyhendid
                from classifications.elabor_analysis
                group by kasutatav_nimetus, t_yhik
              ) as c
where array_length(c.t_lyhendid, 1) > 1;

--näide
-- kas_nim   unit   t_lyhendid
--Albumiin   g/L    S,P-Alb,U-Alb-Fr
--Albumiin % %      S-Alb-Fr%,U-Alb-Fr%
select * from classifications.elabor_analysis
where kasutatav_nimetus = 'Albumiin' or kasutatav_nimetus = 'Albumiin %'
--probleem selles, et ei tea, kas uriinis või seerumis





----------------------------------------------------------------------------------------------------------------------
-- CREATE MAPPINGU parandamine
----------------------------------------------------------------------------------------------------------------------


--set role egcut_epi_analysiscleaning_create;

drop table if exists analysiscleaning.elabor_parameter_name_to_loinc_mapping2;
create table analysiscleaning.elabor_parameter_name_to_loinc_mapping2 (
  parameter_name varchar,
  t_lyhend varchar,
  t_nimetus varchar,
  kasutatav_nimetus varchar,
  loinc_code varchar,
  evidence varchar
);


--reset role;

-- case where parameter_name = t_lyhend
insert into analysiscleaning.elabor_parameter_name_to_loinc_mapping2(parameter_name, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_code, evidence)
  select t_lyhend, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_no, 't_lyhend' from classifications.elabor_analysis;

-- case where parameter_name = t_nimetus and t_nimetus is unique
insert into analysiscleaning.elabor_parameter_name_to_loinc_mapping2(parameter_name, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_code, evidence)
  select t_nimetus, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_no, 't_nimetus' from classifications.elabor_analysis
  where t_nimetus in ( --list of uniquely mapped t_nimetus
                        select t_nimetus from (
                              select t_nimetus, array_agg(distinct t_lyhend) as t_lyhendid
                              from classifications.elabor_analysis
                              group by t_nimetus
                        ) as c
                        where array_length(c.t_lyhendid, 1) = 1
                      );


-- case where parameter_name = kasutatav_nimetus and kasutatav_nimetus is unique
insert into analysiscleaning.elabor_parameter_name_to_loinc_mapping2(parameter_name, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_code, evidence)
  select kasutatav_nimetus, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_no, 'kasutatav_nimetus' from classifications.elabor_analysis
  where kasutatav_nimetus in ( --list of uniquely mapped kasutatav_nimetus
                        select kasutatav_nimetus from (
                              select kasutatav_nimetus, array_agg(distinct t_lyhend) as t_lyhendid
                              from classifications.elabor_analysis
                              group by kasutatav_nimetus
                        ) as c
                        where array_length(c.t_lyhendid, 1) = 1
                      );

-- case where parameter_name = t_lyhend + t_nimetus
insert into analysiscleaning.elabor_parameter_name_to_loinc_mapping2(parameter_name, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_code, evidence)
  select t_lyhend || ' ' || t_nimetus, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_no, 't_lyhend + t_nimetus' from classifications.elabor_analysis;

--mapping kukkus 18 705lt -> 18123

-----------------------------------------------------------------------------------------------------------------------
-- UNITI JÄRGI MAPPING

drop table if exists analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping2;
create table analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping2 (
  parameter_name varchar,
  t_yhik varchar,
  t_lyhend varchar,
  t_nimetus varchar,
  kasutatav_nimetus varchar,
  loinc_code varchar,
  evidence varchar
);

-- case where parameter_name = t_nimetus and t_nimetus + t_unit is unique

-- äkki saab mitmesed mappingud koos UNITIGA üheseks
-- MITMESED mappingud ka koos unitiga 25
-- seega peaks uniti lisamine ära mappima 41-25 = 16

-- T_NIMETUS + UNIT MAPPING
-- 41
insert into analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping2(parameter_name, t_yhik, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_code, evidence)
select t_nimetus, t_yhik,  t_lyhend, t_nimetus, kasutatav_nimetus, loinc_no, 't_nimetus + t_yhik' from classifications.elabor_analysis
where (t_nimetus, coalesce(t_yhik, '')) in (
  select t_nimetus, coalesce(c.t_yhik, '') as t_yhik
  from ( -- kõik ühesed t_nimetus + t_yhik mappingud
         select t_nimetus, t_yhik, array_agg(distinct t_lyhend) as t_lyhendid
         from classifications.elabor_analysis
         group by t_nimetus, t_yhik
       ) as c
  where array_length(c.t_lyhendid, 1) = 1
    and
    -- kui t_nimetuse järgi juba üheselt mappitud, siis pole enam vaja
      t_nimetus not in (
      select parameter_name
      from analysiscleaning.elabor_parameter_name_to_loinc_mapping2
      where evidence = 't_nimetus'
    )
);



-- case where parameter_name = kasutatav_nimetus and kasutatav_nimetus + t_unit is unique
  -- KASUTATAV_NIMETUS + UNIT MAPPING
-- 61 rida
insert into analysiscleaning.elabor_parameter_name_parameter_unit_to_loinc_mapping2(parameter_name, t_yhik, t_lyhend, t_nimetus, kasutatav_nimetus, loinc_code, evidence)
select kasutatav_nimetus, t_yhik,  t_lyhend, t_nimetus, kasutatav_nimetus, loinc_no, 'kasutatav_nimetus + t_yhik' from classifications.elabor_analysis
where (kasutatav_nimetus, coalesce(t_yhik, '')) in (
  select kasutatav_nimetus, coalesce(c.t_yhik, '') as t_yhik
  from ( -- kõik ühesed t_nimetus + t_yhik mappingud
         select kasutatav_nimetus, t_yhik, array_agg(distinct t_lyhend) as t_lyhendid
         from classifications.elabor_analysis
         group by kasutatav_nimetus, t_yhik
       ) as c
  where array_length(c.t_lyhendid, 1) = 1
    and
    -- kui t_nimetuse järgi juba üheselt mappitud, siis pole enam vaja
      c.kasutatav_nimetus not in (
      select parameter_name
      from analysiscleaning.elabor_parameter_name_to_loinc_mapping2
      where evidence = 'kasutatav_nimetus'
    )
);
--\copy (select * from analysiscleaning.elabor_parameter_name_to_loinc_mapping) to 'elabor_mapping_to_loinc_code/elabor_parameter_name_to_loinc_mapping.csv' With (delimiter ',', format csv, header)


-- Still multiple mapping (method required for example)


select * from analysiscleaning.elabor_parameter_name_to_loinc_mapping2
where parameter_name like 'Amikatsiin' or t_nimetus like 'Amikatsiin'

-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Siit tuleb välja probleemne olukord
-- kasutatav_nimetus ütleb et on mitmene, aga t_nimetus teeb üheselt
-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
select * from classifications.elabor_analysis
where kasutatav_nimetus like 'Amikatsiin' or t_nimetus like 'Amikatsiin'

--kõige ülemine päring
; kasutav nimetus
Amikatsiin,mg/L,"{S,P-Amic}"
Amikatsiin,,{Is-Amikacin}
Bakterid,E6/L,{U-Bact Fc}
Bilirubiin,,{U-Bil strip ord}
Candida albicans IgG,mg/L,"{S,P-m5 IgG}"
Candida albicans IgG,,"{S,P-C albicans IgG}"
Epiteelirakud,E6/L,{U-SRC Fc}
Epiteelirakud,,{St-Epiteelirakud}
Erütrotsüüdid,/hpf,{U-Erütrotsüüdid}
Glükoos,,{U-Gluc strip ord}
Hüaliinsilindrid,E6/L,{U-Cast Fc}
Hüaliinsilindrid,/hpf,{U-Hüaliinsilindrid}
Ig fKappa,mg/L,{S-Ig fKappa}
Ig fKappa,,{S-Monoclon-Ig fKappa}
Ig fKappa uriinis,mg/L,{U-Ig fKappa}
Ig fKappa uriinis,,{U-Monoclon-Ig fKappa}
Ig fLambda,mg/L,{S-Ig fLambda}
Ig fLambda,,{S-Monoclon-Ig fLambda}
Ig fLambda uriinis,mg/L,{U-Ig fLambda}
Ig fLambda uriinis,,{U-Monoclon-Ig fLambda}
"Infliksimab (Remicade, Remsia, Inflectra) IgG QN",kU/L,"{S,P-Infliximab IgG QN}"
"Infliksimab (Remicade, Remsia, Inflectra) IgG QN",mg/L,"{S,P-Infliximab IgG QN (mg/L)}"
Ketokehad,mmol/L,{U-Ket strip}
Ketokehad,,{U-Ket strip ord}
Kolistiin,mg/L,"{S,P-Colistin}"
Kolistiin,,{Is-Colistin}
Kristallid,E6/L,{U-Xtal Fc}
Lameepiteeli rakud,/hpf,{U-Lameepiteeli rakud}
Leukotsüüdid,/hpf,{U-Leukotsüüdid}
Lima,E6/L,{U-Mucus Fc}
Lümfotsüüdid,E9/L,{B-Lümfotsüüdid #}
Lümfotsüüdid,/hpf,{U-Lümfotsüüdid}
Lümfotsüüdid,,{XXX-Lümfotsüüdid (mb)}
M-komponent 1,g/d,{dU-M-spike1-Fr}
M-komponent 2,g/d,{dU-M-spike2-Fr}
M-komponent 3,g/d,{dU-M-spike3-Fr}
MN,E9/L,{pDiaF-MN#}
Moksifloksatsiin,mg/L,"{S,P-Moxifloxacin}"
Moksifloksatsiin,,{Is-Moxifloxacin}
Monotsüüdid,E9/L,{B-Monotsüüdid #}
Monotsüüdid,,{XXX-Monotsüüdid}
Neutrofiilid,/hpf,{U-Neutrofiilid}
PMN,E9/L,{pDiaF-PMN#}
Porfüriinid ööpäevases uriinis,nmol/d,{dU-Porph (nmol/d)}
Porfüriinid ööpäevases uriinis,ug/d,{dU-Porph}
Pärmseened,E6/L,{U-Yeast Fc}
Rifampitsiin,mg/L,"{S,P-Rifampicin}"
Rifampitsiin,,{Is-Rifampin}
Seleen,ug/L,{S-Se}
Seleen,umol/L,{P-Se}
Spermatosoidid,E6/L,{U-Sperm Fc}
Spermatosoidid,E9/L,{Sem-Spermatosoidid}
Urobilinogeen,umol/L,{U-Ubg strip}
Urobilinogeen,,{U-Ubg strip ord}
Valk,,{U-Prot strip ord}
Vankomütsiin,mg/L,"{S,P-Vanco}"
Vankomütsiin,,{Is-Vancomycin}
Vitamiin B6,nmol/L,"{S,P-Vit B6}"
Vitamiin B6,ug/L,{B-Vit B6}
Vorikonasool,mg/L,"{S,P-Voricon}"
Vorikonasool,,{Is-Voriconazole}



; --kasutatabv_nimetus not in ...
Bakterid
Bilirubiin
Candida albicans IgG
Candida albicans IgG
Erütrotsüüdid
Glükoos
Hüaliinsilindrid
Hüaliinsilindrid
Ig fKappa
Ig fKappa
Ig fKappa uriinis
Ig fKappa uriinis
Ig fLambda
Ig fLambda
Ig fLambda uriinis
Ig fLambda uriinis
"Infliksimab (Remicade, Remsia, Inflectra) IgG QN"
"Infliksimab (Remicade, Remsia, Inflectra) IgG QN"
Ketokehad
Ketokehad
Kristallid
Lameepiteeli rakud
Leukotsüüdid
Lima
Lümfotsüüdid
Lümfotsüüdid
Lümfotsüüdid
M-komponent 1
M-komponent 2
M-komponent 3
MN
PMN
Pärmseened
Seleen
Seleen
Urobilinogeen
Urobilinogeen
Valk
Vitamiin B6
Vitamiin B6







;Aldosteroon seerumis/plasmas
Aldosteroon seerumis/plasmas
Arteriaalse vere ja sissehingatava õhu hapniku osarõhkude suhe
Arteriaalse vere ja sissehingatava õhu hapniku osarõhkude suhe
Bakterid
Bakterid
Bilirubiin
Bilirubiin
Candida albicans vastane IgG seerumis/plasmas
Candida albicans vastane IgG seerumis/plasmas
Erütrotsüüdid
Erütrotsüüdid
Erütrotsüütide suurusjaotuvus
Erütrotsüütide suurusjaotuvus
Fenüülalaniin vereplekist 11111
Fenüülalaniin vereplekist
Glükoos
Glükoos
Hüaliinsilindrid
Hüaliinsilindrid
Ketokehad
Ketokehad
Kristallid
Kristallid
Lameepiteeli rakud
Leukotsüüdid
Leukotsüüdid
Leukotsüüdid
Lima
Lümfotsüüdid
Lümfotsüüdid
Protrombiini aeg plasmas
Protrombiini aeg plasmas
Pärmseened
Pärmseened
Ribosomaalse P-proteiini vastane IgG seerumis/plasmas
Ribosomaalse P-proteiini vastane IgG seerumis/plasmas
Urobilinogeen
Urobilinogeen
Valk
Valk
