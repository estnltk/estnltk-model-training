# Cleaning entry

Input: table `original.drug_entry` 

Output: table `*_drug_entry_cleaned` 

Requirements:
1) in  `classification ` schema exists table  [`active_substance `](https://git.stacc.ee/project4/classifications/blob/master/ravimiamet/source-data/active-substances.csv).
2) in  `classification ` schema exists table  [`drug_packages `](https://git.stacc.ee/project4/classifications/blob/master/ravimiamet/source-data/drug-packages.csv).


Cleaning involves:
* standardizing `drug_code_display_name` to be in estonian (initially some values are in latin and some in estonian) and renaming it to `active_ingredient`
* adding columns of  `active_ingredient` in estonian, latin and english 
	*  `active_ingredient_est`
	*  `active_ingredient_latin`
	*  `active_ingredient_eng`
* replacing empty strings with`NULL`
* replacing `,` with `.` in `dose_quantity` and `rate_quantity`
* removing trailing whitespaces, commas, hyphens
* converting date from string to timestamp
* using cleaning functions from [step00](https://git.stacc.ee/project4/cda-data-cleaning/tree/master/cda_data_cleaning/data_cleaning/drug_data_cleaning/step00_create_cleaning_functions) (more detailed description found there)
	* `clean_dose_quantity_value(dose_quantity_value_raw)`	
		* modifies column `dose_quantity_value`
		*  modifies column `dose_quantity_unit`
	* `clean_active_ingredient_entry(active_ingredient_raw, drug_code)`
		* modifies column  `drug_code_display_name`
		* creates a new column  `dose_quantity_extra`
		* creates a new column  `package_size`
		


