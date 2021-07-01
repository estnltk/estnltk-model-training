
# Parameter name to t lyhend mapping

**Goal** is to map `t_lyhend` to `work.*_analysis_html` using `parameter_name`.

Each `t_lyhend` has exactly one corresponding LOINC numbers and therefore mapping `t_lyhend`s results in LOINC number mapping.

**Missing value description:** 
In lot of cases `parameter_name` is `NULL`, which means that those HTML tables have been parsed incorrectly and `parameter_name` is under `analysis_name`.



## 1. Cleaning the data

Function that performs the cleaning is under luigi [CleanParameterName](https://git.stacc.ee/project4/cda-data-cleaning/blob/master/analysis_data_cleaning/LOINC_cleaning/luigi_apply_loinc_mapping.py).

Column `before parameter` shows the `parameter name` value before and column `after parameter name` shows the `parameter name` value after the cleaning.

### 1.1 Name cleaning
There are `parameter_name` values starting with 'a' and some number combination. Those combinations are eliminated.

| before parameter name | after parameter name|
|----------------------|---------------------|
|a1 ALAT			   | ALAT |
|a20 ASAT              | ASAT |
|a139 Valk             | Valk |
|a2041 Hb              | HB   |

### 1.2 Removing * from end
When `parameter_name` ends with **\***, the **\*** is eliminated.

| before parameter name | after parameter name|
|----------------------|---------------------|
|Fibrinogeen plasmas*  | Fibrinogeen plasmas |
|Prokaltsitoniin seerumis*             | Prokaltsitoniin seerumis|

### 1.3 Removing parentheses
Works **only** for single pair of parentheses. 
Example:

| before parameter name | after parameter name|
|----------------------|---------------------|
|MCV (erütr.keskm.maht) | MCV | 
| Uc-Aer(Aeroobide uuring uriinist (kateeter)) | Uc-Aer) |

Note that initial `parameter_name` values are kept in the column `parameter_name` and values with removed parentheses are under `parameter_name_raw`.
 
### 1.4 % and # from beginning to end
In some cases `parameter_name` has `%` and `#` in the beginning instead of the end, moving the sign to the end.

| before parameter name | after parameter name|
|----------------------|---------------------|
| %NEUT | NEUT% | 
| #NEUT | NEUT# |


### 1.5 Whitespace unification
Replacing multiple whitespaces with one whitespace.




## 2. Mapping `t_lyhend` using `parameter_name`

### 2.1 Elabor analysis

#### 2.1.1
Using `classifications.elabor_analysis` from [classification repo](https://git.stacc.ee/project4/classifications/tree/master/loinc/elhr-digilugu-ee-algandmed).
Example of columns used from `elabor_analysis`:

| t_lyhend  | kasutatav_nimetus | t_nimetus  |
|-----------|-------------------|------------|
|S,P-ALAT   | ALAT              |Alaniini aminotransferaas seerumis/plasmas|
| B-Hb | Hb | Hemoglobiin |
| P-Fibr | Fibrinogeen | Fibrinogeen plasmas |

In the initial `analysis_html` table some `parameter_name` values are already in the same form as in `t_lyhend` or `kasutatav_nimetus` or `t_nimetus`. So `t_lyhend` can be mapped by joining 
* `kasutatav_nimetus` = `parameter_name` or
*  `t_nimetus` = `parameter_name` 
*  `t_lyhend` = `parameter_name` 

<!--- As a result 2 772 381/4 246 711 = 65.2%  of `analysis_html` are still left unmapped.-->


#### 2.1.2 

In some cases `parameter_name` is a combination of `t_lyhend` and `t_nimetus` like

| parameter_name  | t_lyhend | t_nimetus  |
|-----------------|----------|------------|
| CSF-Alb Albumiin liikvoris | CSF-Alb | Albumiin liikvoris |
| dU-Alb Albumiin ööpäevases uriinis | dU-Alb | Albumiin ööpäevases uriinis | dU-Alb |



Created mapping file `dirty_code_tlyh_and_t_nimetus_to_tlyhend`

| t_lyhend + t_nimetus  | loinc_code(T_luhend) | 
|-----------|-------------------|
| CSF-Alb Albumiin liikvoris |CSF-Alb |
| dU-Alb Albumiin ööpäevases uriinis | dU-Alb | 



So `t_lyhend` can be mapped by joining
* `t_lyhend` + `t_nimetus` = `parameter_name`

Result:

| parameter_name  | loinc_code(T_luhend) | 
|-----------|-------------------|
| CSF-Alb Albumiin liikvoris |CSF-Alb |
| dU-Alb Albumiin ööpäevases uriinis | dU-Alb | 




### 2.2 Replacing special characters
Manual mapping files don't use special characters so for further mapping letters  ä,ö,õ,ü,Ä,Ö,Õ,Ü are replaced with a,o,o,u,A,O,O,U.

### 2.3 Predefined csv file rules

#### 2.3.1 Mapping by `parameter_name`

Under development/name_cleaning_data are different mapping files
* latest_name_cases.csv 
* latest_parentheses_one_cases.csv 
* latest_parentheses_three_cases.csv 
* latest_parentheses_two_cases.csv 	
* latest_prefix_cases.csv 	
* latest_semimanual_cases.csv 	
* latest_manual_cases.csv #peab veel lisama

with two columns `dirty code` and `loinc_code(T-luhend)`. All those files put together was obtained mapping file `dirty_code_to_t_lyhend_mapping`.  

Example rows of table `dirty_code_to_t_lyhend_mapping`.

| dirty_code | loinc_code(T-luhend) |
|----------- |----------------------|
|Alaniini aminotransferaas | S,P-ALAT |
|IG - Ebakupsed granulotsuudid |B-IG#|
|IG Ebakupsete granulotsuudide absoluutarv | B-IG#|
|IG% Ebakupsete granulotsuudide suhtarv | B-IG%|
|IG% - Ebakups.granulotsuutide % |B-IG%|
|Kreatinii(Kreatiniin) |S,P-Crea |
|Kreatinii(Kreatiniin) | S,P-Crea |
|Kreatiniin(Kreatiniin) | S,P-Crea |
|Kreatiniin(Kreatiniin seerumis) | S,P-Crea |

Mapping `loinc_code(T-luhend)` by
* `dirty_code` = `parameter_name_raw` (parameter_name **without** parenthesis)
* `dirty_code` = `parameter_name` (parameter_name **with** parenthesis)

#### 2.3.2 Mapping by `parameter_name` and `parameter_unit`

Used predefined mapping file 
* latest_subspecial_cases.csv 

where mapping is based on both `parameter_name` and `unit`.


| dirty_code | unit | loinc_code(T-luhend) |
|----------- |-------| ---------------|
| PDW | fL |B-PDW |
| PDW | % | B-PDW-CV |



## 3. Mapping `t_lyhend` using `analysis_name`

If `parameter_name` is `NULL` then it is under `analysis_name`.

Step 2.1, 2.2, 2.3 are repeated for `analysis_name` (not `parameter_name`).





# Parameter name to t_lyhend mapping

Goal is to map `t_lyhend` to `work.*_analysis_html` using `parameter_name`.

## 1. Cleaning the data

Column `before parameter` shows the `parameter name` value before and column `after parameter name` shows the `parameter name` value after the cleaning.

### 1.1 Name cleaning
There are `parameter_name` values starting with 'a' and some number combination. Those combinations are eliminated.

| before parameter name | after parameter name|
|----------------------|---------------------|
|a1 ALAT			   | ALAT |
|a20 ASAT              | ASAT |
|a139 Valk             | Valk |
|a2041 Hb              | HB   |

### 1.2 Removing * from end
When `parameter_name` ends with **\***, the **\*** is eliminated.

| before parameter name | after parameter name|
|----------------------|---------------------|
|Fibrinogeen plasmas*  | Fibrinogeen plasmas |
|Prokaltsitoniin seerumis*             | Prokaltsitoniin seerumis|

### 1.3 Removing parentheses
Works **only** for single pair of parentheses. 
Example:

| before parameter name | after parameter name|
|----------------------|---------------------|
|MCV (erütr.keskm.maht) | MCV | 
| Uc-Aer(Aeroobide uuring uriinist (kateeter)) | Uc-Aer) |
Note that initial `parameter_name` values are kept in the column `parameter_name` and values with removed parentheses are under `parameter_name_raw`.
 
### 1.4 % and # from beginning to end
In some cases `parameter_name` has `%` and `#` in the beginning instead of the end, moving the sign to the end.

| before parameter name | after parameter name|
|----------------------|---------------------|
| %NEUT | NEUT% | 
| #NEUT | NEUT# |


### 1.5 Whitespace unification
Replacing multiple whitespaces with one whitespace.

### Should be added
* change "_" to " "


## 2. Mapping `t_lyhend` using `parameter_name`

### 2.1 Elabor analysis

#### 2.1.1
Using `classifications.elabor_analysis` from [classification repo](https://git.stacc.ee/project4/classifications/tree/master/loinc/elhr-digilugu-ee-algandmed).
Example of columns used from `elabor_analysis`:

| t_lyhend  | kasutatav_nimetus | t_nimetus  |
|-----------|-------------------|------------|
|S,P-ALAT   | ALAT              |Alaniini aminotransferaas seerumis/plasmas|
| B-Hb | Hb | Hemoglobiin |
| P-Fibr | Fibrinogeen | Fibrinogeen plasmas |

In the initial `analysis_html` table some `parameter_name` values are already in the same form as in `t_lyhend` or `kasutatav_nimetus` or `t_nimetus`. So `t_lyhend` can be mapped by joining 
* `kasutatav_nimetus` = `parameter_name` or
*  `t_nimetus` = `parameter_name` 
*  `t_lyhend` = `parameter_name` 

<!--- As a result 2 772 381/4 246 711 = 65.2%  of `analysis_html` are still left unmapped.-->


#### 2.1.2 

In some cases `parameter_name` is a combination of `t_lyhend` and `t_nimetus` like

| parameter_name  | t_lyhend | t_nimetus  |
|-----------|-------------------|------------|
| CSF-Alb Albumiin liikvoris | CSF-Alb | Albumiin liikvoris |
| dU-Alb Albumiin ööpäevases uriinis | dU-Alb | Albumiin ööpäevases uriinis | dU-Alb|



Created mapping file `dirty_code_tlyh_and_t_nimetus_to_tlyhend`

| t_lyhend + t_nimetus  | loinc_code(T_luhend) | 
|-----------|-------------------|
| CSF-Alb Albumiin liikvoris |CSF-Alb |
| dU-Alb Albumiin ööpäevases uriinis | dU-Alb | 



So `t_lyhend` can be mapped by joining
* `t_lyhend` + `t_nimetus` = `parameter_name`

Result:

| parameter_name  | loinc_code(T_luhend) | 
|-----------|-------------------|
| CSF-Alb Albumiin liikvoris |CSF-Alb |
| dU-Alb Albumiin ööpäevases uriinis | dU-Alb | 




### 2.2 Replacing special characters
Manual mapping files don't use special characters so for further mapping letters  ä,ö,õ,ü,Ä,Ö,Õ,Ü are replaced with a,o,o,u,A,O,O,U.

### 2.3 Predefined csv file rules

#### 2.3.1 Mapping by `parameter_name`

Under development/name_cleaning_data are different mapping files
* latest_name_cases.csv 
* latest_parentheses_one_cases.csv 
* latest_parentheses_three_cases.csv 
* latest_parentheses_two_cases.csv 	
* latest_prefix_cases.csv 	
* latest_semimanual_cases.csv 	
* latest_manual_cases.csv #peab veel lisama

with two columns `dirty code` and `loinc_code(T-luhend)`. All those files put together was obtained mapping file `dirty_code_to_t_lyhend_mapping`.  

Example rows of table `dirty_code_to_t_lyhend_mapping`.

| dirty_code | loinc_code(T-luhend) |
|----------- |----------------------|
|Alaniini aminotransferaas | S,P-ALAT |
|IG - Ebakupsed granulotsuudid |B-IG#|
|IG Ebakupsete granulotsuudide absoluutarv | B-IG#|
|IG% Ebakupsete granulotsuudide suhtarv | B-IG%|
|IG% - Ebakups.granulotsuutide % |B-IG%|
|Kreatinii(Kreatiniin) |S,P-Crea |
|Kreatinii(Kreatiniin) | S,P-Crea |
|Kreatiniin(Kreatiniin) | S,P-Crea |
|Kreatiniin(Kreatiniin seerumis) | S,P-Crea |

Mapping `loinc_code(T-luhend)` by
* `dirty_code` = `parameter_name_raw` (parameter_name **without** parenthesis)
* `dirty_code` = `parameter_name` (parameter_name **with** parenthesis)

#### 2.3.2 Mapping by `parameter_name` and `parameter_unit`

Used predefined mapping file 
* latest_subspecial_cases.csv 

where mapping is based on both `parameter_name` and `unit`.


| dirty_code | unit | loinc_code(T-luhend) |
|----------- |-------| ---------------|
| PDW | fL |B-PDW |
| PDW | % | B-PDW-CV |



## 3. Mapping `t_lyhend` using `analysis_name`

In lot of cases `parameter_name` is `NULL`, which means that those HTML tables have been parsed incorrectly and `parameter_name` is under `analysis_name`.

Step 2.1, 2.2, 2.3 are repeated for `analysis_name` (not `parameter_name`).






