# Drug text field

This step 
1) parses text field to database
2) cleans the table
3) maps missing `drug_code` and `active_ingredient`  to  `*_drug_parsed` based on Ravimiamet's standard drug table.

## Parse drug text field

Requires estnltk version 1.6.

Input: from table `original.drug` column `text`

Output: table `*_drug_parsed`

There are different types of texts in the table `original.drug` under column `text`.  This step takes `text` column as input and organizes information found in it to a table `*_drug_parsed`.

All the text taggers are [here](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step02_parse_and_clean_drug_text_field/taggers).

Tests for the taggers are [here](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step02_parse_and_clean_drug_text_field/tests).

## Output table

The columns created in the output table  `*_drug_parsed`:

column name | description |example |
-- | -- | --|
id | unique id for each row| 1
epi_id |epicrisis id|1000222
epi_type  | epicrisis type  | s
recipe_code | code of the recipe |2007387104
date| | 18.09.2011
atc_code | | S01AA12
drug_name| name of the drug | DIGOXIN NYCOMED 
active_ingredient| ingredient of the drug| Digoxinum
dose_quantity| how big is the dose | 1/2
dose_quantity_unit| which units is the dose quantity measured (tablett, mg, etc)|tab
rate_quantity| how often should the dose be taken| 1
rate_quantity_unit | which rate should the dose be taken | hommikul
drug_form_administration_unit_code_display_name| type of drug (tablett, süstelahus, pulber etc) | õhukese polümeerikattega tablett
package_size | how many tablets in the package | N120
text_type | which grammar rules type was used for parsing the text | 6



### Text types

In general there are 7 different types of texts.

**Very detailed** rules used by taggers for different text types are described under [grammar_rules.md](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step02_parse_and_clean_drug_text_field/grammar_rules.md). 

In the table below is a **short** overview of different type of texts found in the text field, more examples of raw texts are given under [example_texts](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step02_parse_and_clean_drug_text_field/example_texts).

TYPE NUMBER | DESCRIPTION | EXAMPLE |
-- | -- | -- |
1.1 |First row looks like: 'Retsepti nr.	Ravimi väljastamise kuupäev	Toimeaine	Ravimivorm	Ühekordne annus	Manustamiskordade arv ajaühikus' or 'Retsepti nr.	Ravimi väljastamise kuupäev	Toimeaine	Ravimivorm	ühekordne annus	Manustamiskordade arv ajaühikus' | Retsepti nr.	Ravimi väljastamise kuupäev	Toimeaine	Ravimivorm	Ühekordne annus	Manustamiskordade arv ajaühikus 1018360434	20130412	lispro-insuliin	süstelahus | 
1.2 | First row looks like: 'Retsepti nr.	Toimeaine	Ravimvorm	Annustamine' | Retsepti nr.	Toimeaine	Ravimvorm	Annustamine 1037886058	N05AH04-kvetiapiin	õhukese polümeerikattega tablett	1-2 ööseks | 
2 | Text starts with 'Väljastatud ravimite ATC koodid' | Väljastatud ravimite ATC koodid: L01BA01 B03BB01 | 
3 | Text contains 'Väljastamise kuupäev', 'Retsepti number', 'Ravimi nimetus', 'Originaalide arv' etc (and 'Väljastamise kuupäev', 'Retsepti number' etc) | Väljastamise kuupäev: 12.04.2010, Retsepti number:  1000924652, Ravimi nimetus:  10TK, Originaalide arv: 1, ATC kood: M01AB05, Toimeaine: Diclofenacum, Ravimvorm: rektaalsuposiit, Soodustus: 50%, Annustamine: 1 tk 2 x pv | 
4 | First row is blank followed by 'Kuupäev:.*nimetus/toimeaine' | Kuupäev: 20140710, nimetus/toimeaine: deksametasoon 1MG/1G; 3.5 G silmasalv; 1 TK;  | 
5.1 | 1 to 3 10 digit number seperated by comma in the beginning | 1012730590, 2012730590, 3012730590 19.06.2012 CELEBREX | 
5.2 | starts with a date |  07.09.2015 Esmased varased  jalaortoosid | 
6 | start with (blank line) followed by Rp. | Rp. XEFO TBL 8MG N40 |
7 | starts with drug name or ingredient| Pentoxifyllinum, TRENTAL DEPOTBL 400MG N100 | 
						

## Clean parsed drug table 

Input: table `*_drug_parsed`

Output: table `*_drug_parsed_cleaned`

Cleaning involves
-   standardizing `active_ingredient` to be in estonian (initially some values are in latin and some in estonian)
-   adding columns of `active_ingredient` in estonian, latin and english
    -   `active_ingredient_est`
    -   `active_ingredient_lating`
    -   `active_ingredient_eng`
-   replacing empty strings with`NULL`
-   replacing `,` with `.` in `dose_quantity` and `rate_quantity`
-   removing trailing whitespaces, commas, hyphens
-   converting date from string to timestamp


## Mapping drug code

Input: table `*_drug_parsed_cleaned`

Output: modified `*_drug_parsed_cleaned`

Most of the rows in `*_drug_parsed`  are missing   `drug_code` and  `active_ingredient`, we use [Ravimiamet's table](https://www.ravimiregister.ee/?pv=Andmed.Pakendid) in order to fill those.

Ravimiamet's table is imported to database under name `*_drug_packages`.
Useful columns from ravimiamet's table  are `atc_code`, `active_subtance`, `package_name` and `dosage_form`. 
Corresponding columns in parsed table are

ravimiamet's column | parsed column
--|--
atc_code| drug_code
active_subtance|  active_ingredient
package_name|  drug_name
dosage_form | drug_form_administration_unit_code_display_name

The mapping has 4 parts: 
1. Fill in missing `active_ingredient` based on `package_name` 
2. Fill in missing `active_ingredient` based on `atc_code`
3. Fill in missing `drug_code` based on `active_subtance` and `dosage_form` 
4. Fill in missing `drug_code` based on only `active_subtance`.

