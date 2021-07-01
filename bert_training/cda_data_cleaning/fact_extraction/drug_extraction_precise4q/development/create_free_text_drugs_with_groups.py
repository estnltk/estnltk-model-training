import re
import csv
import psycopg2
import editdistance

from connection_string import connection_string

conn_ut = psycopg2.connect(connection_string)
cur = conn_ut.cursor()

cur.execute("""SELECT * from work.free_text_drugs;""")
tagged_rows = cur.fetchall()


p4q_dict = {}
with open("p4q_drugs.csv", "r") as fin:
    reader = csv.reader(fin)
    for row in reader:
        p4q_dict[row[0].lower()] = row[3]
        # p4q_dict[row[1]] = row[3]
        p4q_dict[row[2].lower()] = row[3]
        p4q_dict[row[3].lower()] = row[3]

dist0_rows = []
dist1_rows = []
dist2_rows = []
dist3_rows = []
dist4_rows = []
ok_rows = []
for idx, row in enumerate(tagged_rows):
    if idx % 1000 == 0:
        print(idx)
    if row[2] == "":
        cleaned_row_drug = re.sub("[0-9]* ?mg", "", row[1])
        cleaned_row_drug = cleaned_row_drug.strip().lower()
        for drug in p4q_dict:

            dist = editdistance.eval(cleaned_row_drug, drug)
            if dist == 0:
                dist0_rows.append([row[0], cleaned_row_drug, drug, p4q_dict[drug]])
            elif dist == 1:
                dist1_rows.append([row[0], cleaned_row_drug, drug, p4q_dict[drug]])
            elif len(row[1]) > 6 and len(drug) > 6 and dist == 2:
                dist2_rows.append([row[0], cleaned_row_drug, drug, p4q_dict[drug]])
            elif len(row[1]) > 8 and len(drug) > 8 and dist == 3:
                dist3_rows.append([row[0], cleaned_row_drug, drug, p4q_dict[drug]])
            elif len(row[1]) > 10 and len(drug) > 10 and dist == 4:
                dist4_rows.append([row[0], cleaned_row_drug, drug, p4q_dict[drug]])
    else:
        new_row = list(row) + ["statin"]
        ok_rows.append(new_row)

print("step1 done")

dist0_drugs = []
dist1_drugs = []
dist2_drugs = []
dist3_drugs = []
dist4_drugs = []
for row in dist0_rows:
    dist0_drugs.append(row[1])
for row in dist1_rows:
    if row[1] not in dist0_drugs:
        dist1_drugs.append(row[1])
for row in dist2_rows:
    if row[1] not in dist0_drugs and row[1] not in dist1_drugs:
        dist2_drugs.append(row[1])
for row in dist3_rows:
    if row[1] not in dist0_drugs and row[1] not in dist1_drugs and row[1] not in dist2_drugs:
        dist3_drugs.append(row[1])
for row in dist4_rows:
    if (
        row[1] not in dist0_drugs
        and row[1] not in dist1_drugs
        and row[1] not in dist2_drugs
        and row[1] not in dist3_drugs
    ):
        dist4_drugs.append(row[1])

dist0_drugs = set(dist0_drugs)
dist1_drugs = set(dist1_drugs)
dist2_drugs = set(dist2_drugs)
dist3_drugs = set(dist3_drugs)

print("step2 done")

for row in dist0_rows:
    ok_rows.append(row)
for row in dist1_rows:
    if row[1] not in dist0_drugs:
        ok_rows.append(row)
for row in dist2_rows:
    if row[1] not in dist0_drugs and row[1] not in dist1_drugs:
        ok_rows.append(row)
for row in dist3_rows:
    if row[1] not in dist0_drugs and row[1] not in dist1_drugs and row[1] not in dist2_drugs:
        ok_rows.append(row)
for row in dist1_rows:
    if (
        row[1] not in dist0_drugs
        and row[1] not in dist1_drugs
        and row[1] not in dist2_drugs
        and row[1] not in dist3_drugs
    ):
        ok_rows.append(row)

print("step 3 done")

ok_rows2 = []
for row in ok_rows:
    new_row = tuple(row)
    ok_rows2.append(new_row)

ok_rows_unique = list(set(ok_rows2))

cur.execute(
    """
drop table if exists work.free_text_drugs_with_p4q_groups;
create table work.free_text_drugs_with_p4q_groups (epi_id text, drug_text text, drug_standard text, drug_p4q text);"""
)

for row in ok_rows_unique:
    cur.execute(
        "insert into work.free_text_drugs_with_p4q_groups VALUES (%s, %s, %s, %s)", (row[0], row[1], row[2], row[3])
    )

conn_ut.commit()
cur.close()
conn_ut.close()
print("all done")
