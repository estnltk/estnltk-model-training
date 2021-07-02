# LOINC - Logical Observation Identifiers, Names, and Codes

As they say themselves at [https://loinc.org/](https://loinc.org/):

> LOINC — The freely available standard for identifying health measurements, observations, and documents.

LOINC's universal observation identifiers can be an essential ingredient for sharing and 
understanding clinical and laboratory data from many sources.

Our sub-goal: We want to enrich analysis results extracted from CDA XML files (epicrises) with LOINC information.


## Steps to produce `analysis_loinced`

![Steps to produce analysis_loinced](development/analysis_loinced.png "Files used to create analysis_loinced")

Our main query maps all the entries in matched table (`analysis_matched`), i.e. records with fields like

* `analysis_name` (example: `Hemogramm 5-osalise leukogrammiga`)
* `parameter_name` (`BASO#`)
* `parameter_code` (`B-CBC+Diff`)
* `unit` (`10\9/L`)

to corresponding LOINC parameters (`analyte` (_aka_ component), `property`, `system`, `time_aspect`, `scale`).

On the top level we have gotten this by joining tables

* `analysis_matched`
* `long_loinc_mappig` (based on CSV file [long_loinc_mapping.csv](long_loinc_mapping.csv))

[analysis_data_cleaning/LOINC_cleaning/main.py](main.py) connects to PostgreSQL database 
and executes join operation (blueprint of which is seen also 
in [development/analysis_loinced.sql](development/analysis_loinced.sql)).

Longer description of how the static LOINC mapping was derived 
is given at [long_loinc_mapping.md](long_loinc_mapping.md).


## Related

* https://git.stacc.ee/project4/classifications/tree/master/loinc
    * Our own mini-classifications (derived from data and various sources) like 
        * `Amyl,Amylase,Amülaas` for anaytes (loinc_analytes.csv)
        * `MRat,Mass rate,Massi suhe` for properties (loinc_properties.csv)
        * `cB,capillary blood,Kapillaarne veri` for systems (loinc_systems.csv)
* https://git.stacc.ee/project4-egcut/LOINCtagger
* [LOINC® - A Universal Catalog of Individual Clinical Observations and Uniform Representation of Enumerated Collections](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3418707/) by Daniel J. Vreeman _et al_, 2010
* [Best Practices for Using LOINC](https://pcornetcommons.org/wp-content/uploads/2017/10/2016-01-LOINC-Introduction-and-Best-Practices-short.compressed.pdf) - slides 
  by Daniel J. Vreeman
* [Advancing Lab Data Interoperability with LOINC](https://ftp.cdc.gov/pub/CLIAC_meeting_presentations/pdf/Addenda/cliac0418/14_Vreeman_Lab_Data_Interop_LOINC.pdf)
  by Daniel J. Vreeman (Aprill 2018)