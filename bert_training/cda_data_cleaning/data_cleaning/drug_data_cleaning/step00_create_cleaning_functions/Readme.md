# Creating cleaning functions

**Purpose**:  create sql functions to clean table `original.drug_entry`. columns
* `drug_code_display_name` (otherwise known as ingredient)
* `dose_quantity_value`.


## Drug code display name

The requirement for using this function is that in `classifications` schema exists table `drug_packages`.

The function returns an array, where first element is cleaned `active_ingredient`, the second element is extra information about dose (later named as  `dose_quantity_extra`) and thrid element is extra infomrmation about package size.

Cleaning contains
- replacing '&#252;' with 'ü' and '&#246;' with 'ö'
- substituting '_' and '�' with 'õ', 'ä', 'ö' or 'ü' using  `drug_packages `  table from  `classification ` schema
        -- amitript_liin -> amitriptüliin
        -- amitript�liin -> amitriptüliin
 - `drug_code_display_name` might contain dose information, this information is extracted
	 - for example (fifth row shows a case where it does not work)

 | drug_code_display_name | active_ingredient | dose_extra|
 ---- | -----| ------| 
 cardace 2,5 mg tablett | cardace | 2,5 mg tablett
   arava 20 mg | araca | 20 mg
   naatriumlevotüroksiin 50.0 mcg  |naatriumlevotüroksiin | 50.0 mcg
deksametasoon 1,0 mg 1,0 ml | deksametasoon | 1,0 mg 1,0 ml
 glargiin-insuliin 300,0 ÜHIK 1,0 ML |  glargiin-insuliin 300,0 ÜHIK 1,0 ML|  NULL 
      
   -  `drug_code_display_name` might contain package information, this information is extracted 
	   - for example

active_ingredient_raw | active_ingredient | dose_extra| package_size | 
 ---- | -----| ------| --|
metforal  500 tablett  500mg n120 |metforal 500 tablett | 500mg | n120
betaloc zok retardtablett  50mg n30 | betaloc zok retardtablett | 50mg | n30


## Dose quantity unit

Column `dose_quantity_value` should only contain numeric values. Function `clean_dose_quantity_value` checks if the column
has some unit placed there. If unit is found, the text is split into two and in the cleaning step the numeric part is placed under  `dose_quantity_value`
and non-numeric part under  `dose_quantity_unit`.

For example for input `dose_quantity_value` = '20mg' the output will be '{20,mg}' and later `dose_quantity_value` = '20'  and `dose_quantity_unit` = 'mg'.
