select analysis_name,
    analysis_substrate_raw_html as analysis_substrate,
    parameter_code_raw_entry as parameter_code,
    parameter_name,
    parameter_unit as unit,
    count(*)
from egcut_epi.analysiscleaning.r07_analysis_matched
group by analysis_name, analysis_substrate, parameter_code, parameter_name, unit