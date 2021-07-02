/*Uriini paneelide templated*/

/*Uriini alla kuuluvaid analüüsi nimesid on kokku 151*/
select * from egcut_epi.analysiscleaning.analysis_classified
where system_type = 'urine';


/*Kokku ridu 98 000*/
select epi_id, analysis_name, parameter_code_raw_entry as parameter_code,  parameter_name, parameter_unit as unit
        from  egcut_epi.analysiscleaning.r07_analysis_matched
        where (epi_id, analysis_name) in
              (/*kõik unikaalsed paneelide (> 1 mõõtmist) templated koos näite epi_id'ga*/
              /*mõni analysis_name kordub, aga see tähendab, et mõõdetud teistsuguseid parameetreid*/
              select epi_id, analysis_name from
                      (
                      /*kõik analüüsi templated, sisaldab korduvaid paneeli template*/
                      select epi_id,
                            analysis_name,
                            array_agg(distinct parameter_name)                                  as param_hulk,
                            row_number() over (partition by array_agg(distinct parameter_name)) as reanumber
                      from egcut_epi.analysiscleaning.r07_analysis_matched
                      /*kõik analüüsinimed, mida loeme uriiniks*/
                      where analysis_name in
                          (
                          select analysis_name from egcut_epi.analysiscleaning.analysis_classified
                          where system_type = 'urine'
                          )
                      group by epi_id, analysis_name
                      ) as tabel1
               where reanumber = 1 and array_length(param_hulk,1) > 1
              )
group by epi_id, analysis_name, parameter_code, parameter_name, unit
/*et poleks duplikaate*/
having count(*) = 1;

