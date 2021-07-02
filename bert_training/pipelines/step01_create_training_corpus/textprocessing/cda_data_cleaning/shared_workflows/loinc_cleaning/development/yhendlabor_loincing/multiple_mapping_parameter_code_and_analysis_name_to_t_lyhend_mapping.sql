-- kui parameter_code
-- B-CBC+Diff, B-CBC-Diff
-- siis tuleb AN järgi vaadata, kas sobib
-- B-CBC-3Diff või B-CBC-5Diff

select * from analysiscleaning.comparing_pc_pn_mappings_on_entry
where t_lyhend_PC like '%B-CBC-3Diff,B-CBC-5Diff%';





select distinct analysis_name, parameter_code_raw, t_lyhend_pc from analysiscleaning.comparing_PC_PN_mappings_on_entry
where t_lyhend_PC like '%B-CBC-3Diff,B-CBC-5Diff%';



select *
from analysiscleaning.dirty_code_to_t_lyhed_mapping_all
where dirty_code like '%emogramm%';

select *
from classifications.elabor_analysis
where t_lyhend like '%CBC-%';
