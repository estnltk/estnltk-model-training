import csv
from estnltk import Text
import psycopg2
from collections import defaultdict
import re
from collections import Counter
from estnltk import Text
import pickle
from tqdm import tqdm
import csv
import editdistance
import os
import sys

from connection_string import connection_string
from taggers.freetext_drug_tagger import Precise4qDrugNameTagger
from taggers.statin_tagger import StatinTagger


tagger = Precise4qDrugNameTagger(output_layer="drug_names2")
statin_tagger = StatinTagger(output_layer="statins")

conn_ut = psycopg2.connect(connection_string)
cur = conn_ut.cursor()

cur.execute("""SELECT * from work.texts""")
texts = cur.fetchall()

tagged_rows = []
for idx, row in enumerate(texts):
    if idx % 1000 == 0:
        print(idx)
    if row[1]:
        if not re.search("(tundl|allerg|resist)", row[1].lower()):
            t1 = Text(row[1])
            tagger.tag(t1)
            statin_tagger.tag(t1)
            if len(t1.drug_names2) > 0:
                for drug in t1.drug_names2:
                    if drug.start < 75:
                        start = 0
                    else:
                        start = drug.start - 75
                    if drug.end > (len(t1.text) - 75):
                        end = len(t1.text)
                    else:
                        end = drug.end + 75
                    context = t1.text[start:end]
                    tagged_rows.append([row[0], drug.text.strip(), "", context, row[2], row[3]])
            if len(t1.statins) > 0:
                for s in t1.statins:
                    if s.start < 75:
                        start = 0
                    else:
                        start = s.start - 75
                    if s.end > (len(t1.text) - 75):
                        end = len(t1.text)
                    else:
                        end = s.end + 75
                    context = t1.text[start:end]
                    tagged_rows.append([row[0], s.text.strip(), s.substance[0], context, row[2], row[3]])

cur.execute(
    """
drop table if exists work.free_text_drugs_with_context;
create table work.free_text_drugs_with_context (epi_id text, drug text, drug_normalized text, context text, "table" text, field text);"""
)

for row in tagged_rows:
    cur.execute(
        "insert into work.free_text_drugs_with_context VALUES (%s, %s, %s, %s, %s, %s);",
        (row[0], row[1], row[2], row[3], row[4], row[5]),
    )
print("all done")
conn_ut.commit()
cur.close()
conn_ut.close()
