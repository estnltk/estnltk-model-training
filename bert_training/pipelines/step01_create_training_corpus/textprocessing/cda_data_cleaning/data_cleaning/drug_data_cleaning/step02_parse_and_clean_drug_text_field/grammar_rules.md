
# Drug grammar

Terminals are written with capital letters

Non-terminals are written with lowercase letters

## Definitions

Terminal | Example
-- | --
ATC_SYM | "ATC kood"
DATE_SYM | "Kuupäev"
INGR_SYM | "Toimeaine", "Nimetus/toimeaine"
RECIPE_SYM  | "Rp."
DRUG_NAME_SYM | "Ravimi nimetus"
ATC_CODE | "S01XA24"
DATE | "12.04.2016"
INGR | "tiamasool"
RECIPE_CODE |  "1013520355"
NUMBER | "10", "10.5", "80/12.5", "80/10"
DOSE_QUANTITY_UNIT | "MG"
RATE_QUANTITY_UNIT |  "PV"
PACKAGE_TYPE | "toimeainet modifitseeritule vabastav kapsel"
DRUG_NAME | "methadone"
PACKAGE_SIZE | "N50"

Note: PACKAGE_TYPE corresponds to column `drug_form_administraion_unit_code_display_name` in `original.drug_entry`


## Rules for terminals

### Dose

Rule | 
-- |
dose = MSEQ(NUMBER) DOSE_QUANTITY_UNIT (not implemented yet!)  |
dose = NUMBER DOSE_QUANTITY_UNIT |
dose = NUMBER | 

Example | 
-- |
"80/12,5 mg"|
TODO: "1-2 tbl" |
"10MG", "10 MG", "1 ta"|
10 |  

### Rate

Rule| 
-- |
rate = NUMBER RATE_QUANTITY_UNIT |

Example | 
--|
1 x pv | 
2 päevas |

### Dose rate

Rule | 
--|
dose_rate = dose rate|
dose_rate = dose RATE_QUANTITY_UNIT |
dose_rate = NUMBER PACKAGE_TYPE RATE_QUANTITY_UNIT|

Example |
--| 
1 ta 1 x pv |
1 tbl päevas |
1 päevas |
 1 kapsel päevas | 


### ATC code

Rule |
-- | 
atc_code = ATC_SYM ATC_CODE |
atc_code = ATC_CODE |

Example | 
-- |
"ATC koodid: L01BA01" |
"L01BA01"| 


### Date

Rule |
-- | 
date = DATE_SYM DATE |

Example | 
-- |
"Kuupäev: 20100728" | 
20100728|



### Recipe

Rule |
-- |
recipe_code = RECIPE_SYM |
recipe_code = MSEQ(RECIPE_CODE) | 

Example | 
--| 
"Rp." |
"1013520355"|

### Drug name

Rule | 
-- |
drug_name = DRUG_NAME_SYM DRUG_NAME |
drug_name = DRUG_NAME |
drug_name = DRUG_NAME_SYM |


Example |
--|
"Ravimi nimetus: ZOLADEX"|
"VALTREX"|
"Ravimi nimetus: ;"


### Drug ingredient dose

Rule |
-- |
drug_ingr_dose = INGR PACKAGE_TYPE dose |
drug_ingr_dose = INGR dose PACKAGE_TYPE|
drug_ingr_dose = INGR PACKAGE_TYPE|
drug_ingr_dose = INGR dose |
drug_ingr_dose = INGR DOSE_QUANTITY_UNIT |

Example|
--|
"simvastatiin õhukese polümeerikattega tablett 10 mg"|
"metoprolool 50MG; toimeainet prolongeeritult vabastav tablett"|
"metoprolool toimeainet prolongeeritult vabastav tablett"|
"varfariin 3MG" |
" hüdroklorotiasiid tablett 10 MG" | 


### Drug name dose
Note: actually does not need to contain dose

Rule |
--|
drug_name_dose = drug_name PACKAGE_TYPE dose | 
drug_name_dose = drug_name DOSE_QUANTITY_UNIT dose |
drug_name_dose = drug_name dose |
drug_name_dose = drug_name |

Example|
--|
"OMSAL CAPS 0,4 MG"|
DOXYCYCLIN  tablett  100mg' |
"OMEPRAZOL SANDOZ 20 MG" |
"VALTREX"|


## Rules for types

### Type 1

Rule | 
-- | 
 TYPE_1_1 = recipe_code date drug_ingr_dose dose_rate |
 TYPE_1_1 = recipe_code date drug_ingr_dose rate | 
 TYPE_1_1 = recipe_code date drug_ingr_dose | 
 TYPE_1_3 = recipe_code atc_code drug_ingr_dose rate |	
 TYPE_1_3 = recipe_code atc_code drug_ingr_dose  |	

Type | Example | 
 -- | --|
1.1 | 1018217714 20130405 telmisartaan+hüdroklorotiasiid tablett 80/12,5 mg 1 x pv |
1.1 | 1025393284 20140415 metoprolool toimeainet prolongeeritult vabastav tablett 1 ta 2 x pv |
1.1 |1002965348 20101116 omeprasool gastroresistentne kõvakapsel  | 
1.3 | 1031878784 N06AB06- sertraliin õhukese polümeerikattega tablett 1 päevas
1.3 |1033706035 N06AX16- venlafaksiin toimeainet prolongeeritult vabastav kõvakapsel|
1.3 | N05AH04-kvetiapiin õhukese polümeerikattega tablett 1-2 tbl

### Type 2

Rule | 
--|
 TYPE_2 -> atc_code |
 
Example |
--|
Väljastatud ravimite ATC koodid: C01DA14 C10AA07 C07AB02 |

###  Type  3 

Rule |
-- |
 TYPE_3 ->date recipe_code drug_name_dose SEQ(NUMBER) atc_code drug_ingr_dose dose_rate |
 TYPE_3 -> date recipe_code drug_name_dose SEQ(NUMBER) atc_code drug_ingr_dose|

Example | Comment |
-- | -- |
Väljastamise kuupäev: 28.05.2013, Retsepti number:  1019262996, Ravimi nimetus:  56TK, Originaalide arv: 1, ATC kood: A02BC01, Toimeaine: Omeprazolum, Ravimvorm: gastroresistentne kõvakapsel, Soodustus: 50%, Annustamine: 1 ta 1 x pv, Annustamine täpsemalt: 1 tab hommikul. | dose under 'Annustamine täpsemalt'| 
Väljastamise kuupäev: 26.11.2012, Retsepti number:  1015592331,  Ravimi nimetus: AMANTADIN-RATIOPHARM  100 MG 100TK, Originaalide arv: 1,  ATC kood: N04BB01, Toimeaine: Amantadinum, Ravimvorm: õhukese polümeerikattega tablett, Soodustus: 50%, Annustamine:  , Annustamine täpsemalt: 1 tablett päevas. | One dose is under 'Ravimi nimetus', other dose under 'Annustamine täpsemalt', right now using the second one| 
Väljastamise kuupäev: 02.02.2011, Retsepti number:  1003978133, Ravimi nimetus: ZOLADEX 1TK, Originaalide arv: 1,ATC kood: L02AE03, Toimeaine: Goserelinum, Ravimvorm: implantaat,Soodustus: 75%, Annustamine:  , Annustamine täpsemalt:  süstida s/c skeemi järgi.  | No dose or rate at all |



### Type 4

Rule | 
-- | 
TYPE_4 -> date drug_ingr_dose dose |
TYPE_4 -> date drug_ingr_dose |

Example|
--|
Kuupäev: 20120120, nimetus/toimeaine: klonasepaam 2 MG; tablett; 30 TK;  |
Kuupäev: 20140221, nimetus/toimeaine: medroksüprogesteroon 150 MG/1 ML; DEPO-PROVERA 1 ML süstesuspensioon; 1 TK; 1 orig. |
TODO : Kuupäev: 20120312, nimetus/toimeaine: budesoniid 200 MCG/1ANNUST; 200 < ANONYM id="2" type="per" morph="_S_ sg p"/> inhaleeritav pulber; 1 TK;  |
TODO: Kuupäev: 20140423, nimetus/toimeaine: tiamiin 50MG/1ML + püridoksiin 50MG/1ML + tsüanokobalamiin 0.5MG/1ML; + lidokaiin 10MG/1ML; 2 ML süstelahus; 10 TK;  |


###  Type 5

Rule | 
-- |
 TYPE_5_1 -> recipe_code date drug_name_dose	 | 
 TYPE_5_2 -> date drug_name_dose |

Type | Example | 
-- | -- |
5.1 | 1010686329, 2010686329, 3010686329 07.03.2012 ALLOPURINOL NYCOMED | 
5.1 | 1017000739 05.02.2013 KLACID SR 500 MG |
5.2 | 15.03.2015 OMEPRAZOL SANDOZ 20 MG |
5.2 | 18.06.2013 ZIBOR


### Type 6			

Rule | 
-- | 
 TYPE_6 -> recipe_code drug_name_dose PACKAGE_SIZE | 
 TYPE_6 -> recipe drug_name_dose |

Example | 
--|
Rp. IBUMETIN TBL 400MG N100|
Rp. RANIBERL TBL 150MG N50|
Rp. BIOPAROX AEROS 10ML |

Problematic | 
-- |
Rp. Aciclovirum,      UNG OPHTH 3%  4,5G|
Rp. MILGAMMA 100 KAETUD TBL 100MG + 100MG N30|

Note: in the first rule are multiple doses, not sure what to do with the second one

### Type 7

Rule |
-- |
 TYPE_7 -> ingr drug_name_dose PACKAGE_SIZE |
 TYPE_7 -> drug_ingr_dose PACKAGE_SIZE |
 TYPE_7 -> drug_name_dose PACKAGE_SIZE|

Example | 
-- |
Pentoxifyllinum, TRENTAL DEPOTBL 400 MG N100 |
Amlodipinum TBL 5MG N60 |
DOXYCYCLIN  tablett  100mg N10|


Problematic |
-- |
Olmesartanum + Hydrochlorothiazidum, MESAR PLUS TBL 20/12,5MG N28 |
Nitrendipinum, LUSOPRESS TBL 20MG N28 2 origRp. < ANONYM id="2" type="per" morph="_H_ sg n,_H_ pl n;_H_ sg n,_H_ pl n"/> < ANONYM id="3" type="per" morph="_H_ sg n"/> 1MG/G 30GRp. FUCIDIN CR 2% 15G | 
Fluocortolonum + Lidocainum, DOLOPROCT REKTAALKREEM 1MG+20MG/G 15G |

Note: 
* actually belongs under type 6... ?





