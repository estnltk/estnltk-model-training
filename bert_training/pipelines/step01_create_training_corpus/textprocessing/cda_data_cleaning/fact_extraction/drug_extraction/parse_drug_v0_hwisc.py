#!/usr/bin/env python
# coding: utf-8

# In[1]:


from estnltk import Text
import csv


# In[2]:


from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction.drug_extraction.taggers.drug_tagger import DrugFieldPartTagger, DrugTagger
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import create_connection
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import read_config


# In[3]:


part_tagger = DrugFieldPartTagger()


# In[4]:


drug_tagger = DrugTagger()


# In[5]:


config_file = "egcut_epi.ini"
config = read_config(config_file)

conn = create_connection(config)
cur = conn.cursor()


# In[6]:


cur.execute("""SELECT * from original.drug;""")


# In[7]:


drug_rows = cur.fetchall()


# In[8]:


# new_rows = []
with open("parsed_drugs.csv", "a") as fout:
    writer = csv.writer(fout)
    for idx, row in enumerate(drug_rows):
        if idx % 10000 == 0:
            print(idx)
        if row[3]:
            t = Text(row[3]).tag_layer(["words"])
            part_tagger.tag(t)
            drug_tagger.tag(t)

            for j in t.parsed_drug:
                date = ""
                atc_code = ""
                drug_name = ""
                active_ingredient = ""
                if j.date:
                    date = j.date
                if j.atc_code:
                    atc_code = j.atc_code
                if j.drug_name:
                    drug_name = j.drug_name
                if j.active_ingredient:
                    active_ingredient = j.active_ingredient
                # new_rows.append([row[0], row[1], row[2], date, atc_code, drug_name, active_ingredient])
                writer.writerow([row[0], row[1], row[2], date, atc_code, drug_name, active_ingredient])

print("Step 1 done.")
