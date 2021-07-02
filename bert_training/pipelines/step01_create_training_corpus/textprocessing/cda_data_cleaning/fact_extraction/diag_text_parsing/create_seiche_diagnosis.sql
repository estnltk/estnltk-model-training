drop table if exists seiche.diagnosis;

create table seiche.diagnosis as
select id, diag_medical_type, diag_code_raw, diag_name_raw, diag_text_raw, diag_code, diag_name,
diag_name_additional, cleaning_state from work.runfull201903041255_diagnosis;

grant select on seiche.diagnosis to seiche;
