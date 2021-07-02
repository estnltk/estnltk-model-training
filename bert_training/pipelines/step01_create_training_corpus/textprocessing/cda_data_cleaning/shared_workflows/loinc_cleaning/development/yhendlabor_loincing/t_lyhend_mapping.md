**Eesmärk**

Mappida ära `parameter_code`le vastavad T-lühendid (`t_lyhend`). 
Täpsem info T-lühendi, T-nimetus ja kasutatava_nimetuse kohta: 
* classifications/loinc/elhr-digilugu-ee-algandmed/docs/LOINC-work.analysis-tabeli-kontekstis_2017-01-09.md 

Võrrelda 
* long_loinc_mappinguga ära mapitud LOINCi `property`
* `t_lyhendi` abil mapitud `property`

**t_lyhendi mappimine parameter_code kaudu**

Parameter koodi abil t_lyhendi mappimiseks lõin tabel yhendlabor_loincing all `parameter_code_to_t_lyhend_mapping.csv`
Kasutatud tabelid:
* Ühendlabori LOINCI andmed classifications/loinc/elhr-digilugu-ee-algandmed/ 



Näide tabelist

| parameter_code | t_lyhend |
|-------------- |---------|
| P-APTT | P-APTT | 
| PrcF-WBC | PrcF-WBC | 
| S-AST | S,P-ASAT | 
| BASO | B-Baso# | 
| EO% | B-Eo% | 

Tabeli sisu on saadud:
* `long_loinc_mapping`'st, kui 




select distinct parameter_code, t_lyhend
    from analysiscleaning.long_loinc_mapping as l
    left join classifications.elabor_analysis as c
        on c.t_lyhend = l.parameter_code
    where t_lyhend is not null
--order by t_lyhend asc;











------------------------------------------------------------
___________________________________________________________



cda-data-cleaning/analysis_data_cleaning/name_cleaning/data


* uurida, kust tulevad cda-data-cleaning analysis_data_cleaning name_cleaning data failid, nt commiti messagete järgi jne ja mis reegleid nad täpselt defineerivad
* kopeeri reeglite failid LOINC_cleaning/developmendi alla uude kausta
* teha tabel (tb1)
    * parameter code | t_lyhend
    * A (long_loinc_mapping (LLM)) | A (elabor)
    * B (reeglid data all) | C (elabor)
nüüd saab teha kontrolli:
1. analysis_html ----LLM ---> loinc_property
2. analysis_html ----t_lyhend (tabelist tb1) ---> loinc_property