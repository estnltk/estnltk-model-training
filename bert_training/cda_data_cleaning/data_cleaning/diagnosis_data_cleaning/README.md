#Scripts for cleaning diagnosis data.

## Running the whole pipeline

The whole `diagnosis_data_cleaning` pipeline can be executed with the following commands in terminal.

In first terminal window start luigi daemon
```
luigid
```

In the second terminal window execute (define own prefix and configuration file)
```
export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH

prefix="run_201909261208"
conf="configurations/egcut_epi_microrun.example.ini"

luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.diagnosis_data_cleaning.run_diagnosis_data_cleaning RunDiagnosisDataCleaning --prefix=$prefix --config=$conf --workers=1 --log-level=INFO
```

## Analysis steps

**Step 1:** 

The views **original.main_diagnosis**, **original.complication_diagnosis**, **original.by_illness_diagnosis**, **original.outer_cause_diagnosis** and **original.other_diagnosis** are merged into a table **work._prefix__diagnosis**, where _prefix_ changes incrementally (r01, r02, etc).

**Step 2:** Columns in the table are:

* **id** - autoincrementing id
* **src_id** - id from original table
* **epi_id** - epicrisis id
* **epi_type** - ambulatory (a) or stationary (s)
* **diag_medical_type** - main diagnosis (main), complications (complication), co-occurring illnesses (by_illness), outer causes (outer_cause), other (other)
* **diag_code_raw** - diagnosis code (ICD10), uncleaned
* **diag_name_raw** - diagnosis name, uncleaned
* **diag_text_raw** - text sumbitted with diagnosis, uncleaned
* **diag_statistical_type_raw** - statistical type, uncleaned
* **diag_code** - diagnosis code (ICD10), cleaned
* **diag_name** - diagnosis name, cleaned (represents the part of ICD10 name that matched the raw name, not the full name corresponding to ICD10 - that can be achieved by mapping code with ICD10 name).
* **diag_name_additional** - additional information extracted from the diagnosis name
* **diag_statistical_type** - statistical type (only present for main diagnosis), cleaned value has codes
    * first time diagnose - name: **+**, code: **1**
    * reoccurring diagnose - name: **-**, code: **2**
    * initial diagnose - name: **0**, code: **3**
    * None or _määramata_ etc: code: **4**
* **clean** - if the row is cleaned or not
* **cleaning_state** - information about the cleaning state

**Step 3:** 6 last columns are populated with cleaned values (cleaned as much as possible for now)

## Cleaning states

**General cleaning states which are considered cleaned:**

* **exact match** - name is matching exactly
* **similar match** - name is matching with some mistakes 
    * Anne Vassiljeva vs Anna Vassiljeva
    * Lyme i tõbi vs lyme'i tõbi
    * SURJU TERVISEKESKUS vs Osaühing  Surju Terviskeskus
* **version match** - some version of the name is matching (for example reordering first and last name etc)
    * PÄRTMA KAJA vs Kaja Pärtma, M. BRODNEVA vs Maria Brodneva
    * OÜ ELVA KESKLINNA PAK vs Osaühing Elva Kesklinna Perearstikeskus
* **similar version match** - some version of the name is matching with some mistakes
    * J. DO_SLOVAJA vs Jelena Dõšlovaja
    * O� KIVILINNA PAK vs OSAÜHING KIVILINNA PEREARSTIKESKUS

**General cleaning states which are considered not cleaned:**

* **name mismatch** - name didn't match the code
* **code is None** - code is None
* **name is None** - name is None
* **code and name are None** - both code and name are None
* **unknown code** - code does not exist in our list of codes

**Cleaning states which are considered cleaned and are specific to diagnosis cleaning:**

* **prefix match** - the beginning of the diagnosis name uniquely matches to the code
* **_..._; parent code** - name of the diagnosis matches exactly to the parent diagnosis code
* **parents joined match** - name of the diagnosis matches to the concatenated names of diagnosis parent codes
* **parents joined similar match** - same as previous, but matching with some mistakes

**Cleaning states which are considered not cleaned and are specific to diagnosis cleaning:**

_Consists of two parts, they are explained here separately. One of the parts could be ok, but one of them is not ok and causes the row to be not cleaned._

* **> 1 _..._ match** - more than 1 name matched (can be a prefix or a similar match as well), so we cannot clean the row
* **name sub of ICD10** - name is subname of ICD10 name
* **ICD10 sub of name** - ICD10 name is sub of name
* **parts in ICD10** - parts of the name (single words) exist in the corresponding ICD10 name
* **no match** - none of the other cleaning states applied, but wasn't cleanable
