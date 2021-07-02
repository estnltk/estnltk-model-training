drop table if exists seiche.procedures_entry;

create table seiche.procedures_entry as
select
"id", raw_text, epi_type, "schema", "table", "field", row_id, effective_time, header

from work.run_201905311256_extracted_events
where "table" = 'procedures_entry' ;

grant select on seiche.procedures_entry to seiche;
