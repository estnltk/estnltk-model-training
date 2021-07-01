-- Code from https://git.stacc.ee/project4/cda-data-cleaning/blob/master/analysis_html_parsing/step05_match_entry_and_html.py

------------------------------------------------------------------------------------------------------------------------
-- ENTRY
------------------------------------------------------------------------------------------------------------------------

/*
[entry.c[x] for x in columns] + [entry.c.id,
                                    sq.func.row_number().over(partition_by=columns).label("row_nr"),
                                    sq.func.count().over(partition_by=columns).label("row_count"),
                                    sq.func.count().over(partition_by=entry_info_cols).label("equal_rows"),
                                    entry.c.id], entry)\
                            .where(entry.c.html_match_id == None)\
                            .alias("entry_win")

required = ["epi_id"]
optional = ["analysis_name", "value", "parameter_name", "parameter_unit", "reference_values", "effective_time"]
entry_info_cols = required + optional + ["parameter_code_raw"]
*/

--row count - kui palju on ridu, kus ühtivad AN, PN, PU, ET, RV, V
--equal rows - kui palju on ridu, kus KÕIK ühtib (kaasa arvatud parameter_code)

select * from (
    SELECT epi_id, analysis_name, value, parameter_name, reference_values, effective_time, id, (parameter_code_raw),
       row_number() OVER (PARTITION BY epi_id, analysis_name, value, parameter_name, reference_values, effective_time) AS row_nr,
       count(*) OVER (PARTITION BY epi_id, analysis_name, value, parameter_name, reference_values, effective_time) AS row_count,
       count(*) OVER (PARTITION BY epi_id, analysis_name, value, parameter_name, reference_values, effective_time, parameter_code_raw) AS equal_rows
    FROM work.run_201908081103_analysis_entry_cleaned
    ) as entry
where equal_rows != row_count;




select * from (
                  select epi_id,
                         analysis_name,
                         parameter_name,
                         parameter_unit_raw,
                         effective_time,
                         reference_values,
                         value,
                         ROW_NUMBER()
                         OVER (PARTITION BY epi_id, parameter_unit_raw, effective_time, value) as row_nr,
                         count(*)
                         OVER (PARTITION BY epi_id, parameter_unit_raw, effective_time, value) as row_count,
                         count(*)
                         OVER (PARTITION BY epi_id, analysis_name, parameter_name, parameter_unit_raw, effective_time, reference_values,value, parameter_code_raw) as equal_rows
                  from work.run_201908081103_analysis_entry_cleaned
                  limit 5000
              ) as entry
where equal_rows != row_count