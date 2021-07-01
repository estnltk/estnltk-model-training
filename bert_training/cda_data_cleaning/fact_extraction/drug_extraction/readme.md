### Drug extraction from table original.drug

#### Luigi commands
In first terminal window start luigi daemon
```
luigid
```

In the second terminal window execute (define own prefix and configuration file)
```
export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH

prefix="run_202103310800"
conf="configurations/egcut_epi_microrun.example.ini"

luigi --scheduler-port 8082 --module cda_data_cleaning.fact_extraction.drug_extraction.parse_drug ParseDrug \
--prefix=$prefix \
--config-file=$conf \
--workers=1 \
--log-level=INFO
```

This creates the final table `<prefix>_drug_parsed_v1`



Notes:

General:
* Drug codes and dates are tagged based on regexes
* Drug names and active ingredients are tagged based on the lexicon at https://git.stacc.ee/project4/cda-data-cleaning/blob/master/cda_data_cleaning/fact_extraction/drug_extraction/taggers/drug_vocabulary_v2.csv which combines the drug lexicons from https://git.stacc.ee/project4-egcut/precise4q/blob/master/attempts_to_write_taggers/drug_vocabulary.csv and https://git.stacc.ee/project4-egcut/epicrisis-drug-reactions/blob/master/allergiablokk/drug_vocabulary_extended3.csv. As the latter does not distinguish between drug names and ingredients, they were all labelled as drug names, although some of them actually aren't
* Grammar defines the order in which the symbols (date, drug code, drug name, active ingredient) have to occur to be extracted as drugs

Egcut:
* Notebook parse_drug_v1.ipynb contains the code for running the taggers (alternative to luigi) in the taggers folder and putting results into work.drug_parsed_v1 table (needs python 3.6 and estnltk 1.6.4 or sth, runs about 1,5h)

Hwisc:
* Script parse_drug_v0_hwisc.py contains the code for running the taggers in the taggers folder and writing the results into "parsed_drugs.csv". It needs python 3.6 and estnltk 1.6.4 or sth and runs about 36h on p12.
* Notebook write_drugs_into_base_hwisc.ipynb contains the code for cleaning the extracted dates and writing results into work.drug_parsed_v0 table. Runs about 0,5h on p12.

Known problems:
* select * from work.drug_parsed_v0 where length(date_raw) > 2 and date is null; finds 4 rows where parsing fails on hwisc and 1 row on egcut. Problem in date_tagger/grammar, needs further inspection.

Possible improvements to do in the future:
* change the grammar so that it would take into account the text between the symbols. (Current solution: close your eyes and hope that the tagged dates and drugs belong together. So far, there's no empirical evidence that this problem exists)
* find words from text that are similar to existing drug names (edit distance) and add to lexicon so that those would be tagged as well
* figure out a way to distinguish drug names and ingredients properly 
* move clean_date() function from the notebook to taggers/drug_tagger.py
* add ATC codes to drug names/ingredients for which one was not found from text (complicated as there's multiple ATC codes for one drug)
