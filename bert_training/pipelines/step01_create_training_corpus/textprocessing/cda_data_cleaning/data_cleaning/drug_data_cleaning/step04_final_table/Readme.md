# Final table

Input: table  `*_drug_matched_wo_duplicates`
 
Output: final table `*_drug_cleaned`

Creates a final table based on matched table. Matched table has two versions of most of the columns (e.g. `effective_time_entry`, `effective_time_parsed`). This step merges the same named columns together (e.g creates column `effective_time`).

As `*_entry` columns have more trustworthy information, then always prefer `*_entry` column. When `*_entry` is NULL, then `*_parsed` value is chosen.

Columns `dose_quantity_value` (DOV) and `dose_quantity_unit` (DOU) contain information in different forms. For example
* parsed: `dose_quantity_value_parsed = 2` and `dose_quantity_unit_parsed = tabletti` 
* entry: `dose_quantity_value_entry = 10` and  `dose_quantity_value_entry = mg`

As for OMOP mapping dosages given with "tablett" (and not "mg") are more useful for calculating the drug exposure time  then we always prefer the column with values like "tablett".  If neither of the columns has "tablett" then choose the one where there are less NULLs in DOV and DOU. Raw values are also kept in the final table in case more detailed information about dose is needed

For columns `dose_quantity_value` and `dose_quantity_unit` both `*_entry` and  `*_parsed` column seemed to contain useful information. Therefore both are kept.
 For example 
* `dose_quantity_value_parsed = 2` and `dose_quantity_unit_parsed = tabletti` 
* `dose_quantity_value_entry = 10` and  `dose_quantity_value_entry = mg`
This can be exactly the same information kept in different ways, therefore can not decide which one is "better" and should be kept. 

## Output table description

In the following table all the columns of `*_drug_cleaned` are described.

Some columns exist only in either `*_drug_entry` or `*_drug_parsed`, but not in both. Therefore, origin column in the following table indicates if the column is result of merging two tables or is unique for either of the tables.

column_name | description | example | origin
--|--|--|--
epi_id | epicris id  | both
id_entry | id for looking up corresponding row in `*_drug_entry_cleaned` | entry
id_parsed | id for looking up corresponding row in `*_drug_parsed_cleaned` | parsed
id_original_drug | id for looking up corresponding row in `original.drug` table `text` column (`*_drug_parsed_cleaned` table is based on `original.drug`)` | parsed
epi_type | epicrisis type: a - ambulatory, s - stationary | both
effective_time | date | both
drug_code | atc code | both 
active_ingredient | ingredient in estonian, if there is no estonian name, then the original (raw) name is kept  | both
active_ingredient_est | ingredient name in estonian| both
active_ingredient_latin | ingredient name in latin | both
active_ingredient_eng | ingredient name in english | both
drug_name_parsed | name of the drug | parsed
dose_quantity_value | how big is the dose | both
dose_quantity_unit | in which units is the dose given | both
dose_quantity_extra_entry | extra information about dose extracted from drug_code_display_name (=active_ingredient) column | entry
rate_quantity_value | how often should the dose be taken | both
rate_quantity_unit | which rate should the dose be taken | both
dosage_form | which form are the doses given (tablett, süstelahus, pulber etc) | both
drug_id_entry | | entry
prescription_type_entry | 'CURE' or 'PRE', note that this information is probably wrong for cases where epi_type = 'a' and prescription_type = 'CURE' | entry
drug_form_administration_unit_code_entry | either a code consisting of numbers or containing values similar to drug_form_administration_unit_code_display_name | entry
recipe_code_parsed | recipe code | parsed
package_size_parsed | number of tablets in the package | parsed
text_type_parsed  | which grammar rules type was used for parsing the text | parsed 
match_description | which columns were used to match parsed and entry table| both
dose_quantity_value_raw_parsed | raw  dose_quantity_value | parsed
dose_quantity_unit_raw_parsed | raw  dose_quantity_unit  | parsed
dose_quantity_value_raw_entry | raw  dose_quantity_value  | entry
dose_quantity_unit_raw_entry | raw  dose_quantity_unit  | entry
rxnorm_concept_id | concept id of rxnorm based on atc code | both


# Validation

Validation step logs information about column values in the final table `*_drug_cleaned`. Results are placed under folder "output".
