drop table if exists analysiscleaning.analysis_panel_templates;

create table analysiscleaning.analysis_panel_templates AS
select epi_id, analysis_name, parameter_code_raw_entry as parameter_code,  parameter_name, parameter_unit as unit
        from  egcut_epi.analysiscleaning.r07_analysis_matched
        where (epi_id, analysis_name) in
              (/*kõik unikaalsed paneelide (> 1 mõõtmist) templated koos näite epi_id'ga*/
              select epi_id, analysis_name
               from (select epi_id,
                            analysis_name,
                            array_agg(distinct parameter_name)                                  as param_hulk,
                            row_number() over (partition by array_agg(distinct parameter_name)) as reanumber
                     from egcut_epi.analysiscleaning.r07_analysis_matched
                     group by epi_id, analysis_name
                    ) as tabel1
               where reanumber = 1 and array_length(param_hulk,1) > 1
              )
group by epi_id, analysis_name, parameter_code, parameter_name, unit
/*et poleks duplikaate*/
having count(*) = 1;

/*Salvestame csv*/
select * from analysiscleaning.analysis_panel_templates
where analysis_name in
    (select analysis_name from
        (
        select epi_id, analysis_name, count(*) from analysiscleaning.analysis_panel_templates
        group by epi_id, analysis_name
    ) as table1
where count > 1
);
