import re
import psycopg2

from estnltk import Text

from connection_string import connection_string
from taggers.freetext_drug_tagger import Precise4qDrugNameTagger
from taggers.statin_tagger import StatinTagger


tagger = Precise4qDrugNameTagger(output_layer="drug_names2")
statin_tagger = StatinTagger(output_layer="statins")

conn_ut = psycopg2.connect(connection_string)
cur = conn_ut.cursor()

cur.execute("""SELECT * from original.anamnesis""")
anamnesis = cur.fetchall()

epis_and_texts = []
for row in anamnesis:
    epis_and_texts.append([row[1], row[5]])
    epis_and_texts.append([row[1], row[7]])
    epis_and_texts.append([row[1], row[10]])
    epis_and_texts.append([row[1], row[11]])

cur.execute("""SELECT * from original.summary""")
summary = cur.fetchall()

for row in summary:
    epis_and_texts.append([row[1], row[3]])
    epis_and_texts.append([row[1], row[5]])

tagged_rows = []
for idx, row in enumerate(epis_and_texts):
    if idx % 1000 == 0:
        print(idx)
    if row[1]:
        if not re.search("(tundl|allerg|resist)", row[1].lower()):
            t1 = Text(row[1])
            tagger.tag(t1)
            statin_tagger.tag(t1)
            if len(t1.drug_names2) > 0:
                for drug in t1.drug_names2:
                    tagged_rows.append([row[0], drug.text.strip(), ""])
            if len(t1.statins) > 0:
                for s in t1.statins:
                    tagged_rows.append([row[0], s.text.strip(), s.substance[0]])

cur.execute(
    """
drop table if exists work.free_text_drugs;
create table work.free_text_drugs (epi_id text, drug text, drug_normalized text);"""
)

for row in tagged_rows:
    cur.execute("insert into work.free_text_drugs VALUES (%s, %s, %s);", (row[0], row[1], row[2]))
print("all done")
conn_ut.commit()
cur.close()
conn_ut.close()
