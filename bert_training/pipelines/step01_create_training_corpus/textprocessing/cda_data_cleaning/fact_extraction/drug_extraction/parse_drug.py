import luigi
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob

from estnltk import Text
from datetime import datetime
from tqdm import tqdm
from psycopg2 import sql

from .taggers.drug_tagger import DrugFieldPartTagger, DrugTagger


class ParseDrug(CDAJob):
    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        part_tagger = DrugFieldPartTagger()
        drug_tagger = DrugTagger()

        table_name = "drug_parsed_v1"

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        self.log_current_action("Querying data from {}.drug".format(str(self.original_schema)))

        cur.execute(
            sql.SQL(
                """
            SELECT * from {original_schema}.drug;
            """
            ).format(
                original_schema=sql.Identifier(self.original_schema)
            )
        )

        drug_rows = cur.fetchall()

        self.log_current_action("Tagging data")

        new_rows = []
        for row in tqdm(drug_rows):
            if row[3]:
                t = Text(row[3]).tag_layer(['words'])
                part_tagger.tag(t)
                drug_tagger.tag(t)

                for j in t.parsed_drug:
                    date = ''
                    atc_code = ''
                    drug_name = ''
                    active_ingredient = ''
                    if j.date:
                        date = j.date
                    if j.atc_code:
                        atc_code = j.atc_code
                    if j.drug_name:
                        drug_name = j.drug_name
                    if j.active_ingredient:
                        active_ingredient = j.active_ingredient
                    new_rows.append([row[0], row[1], row[2], date, atc_code, drug_name, active_ingredient])

        def clean_date(date_str):
            '''
            Turn datestring into datetime object
            '''
            if date_str:
                stripped_date = date_str.strip('\t\n, ')
            else:
                return
            if len(stripped_date) == 8:
                date = datetime.strptime(stripped_date, '%Y%m%d')
                return date
            elif len(stripped_date) == 10:
                date = datetime.strptime(stripped_date, '%d.%m.%Y')
                return date
            else:
                return

        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {work_schema}.{target_table};
            create table {work_schema}.{target_table}(
            id bigint,
            epi_id text,
            epi_type text,
            date_raw text,
            date date,
            ATC text,
            drug_name text,
            active_ingredient text
        );
            """
            ).format(
                role=sql.Literal(self.role),
                work_schema=sql.Identifier(self.work_schema),
                target_table=sql.Identifier(str(self.prefix) + "_" + table_name)
            )
        )

        self.log_current_action("Inserting tagged data to {}.{}".format(
            str(self.work_schema),
            str(self.prefix) + "." + table_name)
        )

        for row in tqdm(new_rows):
            new_date = clean_date(row[3])
            cur.execute(
                sql.SQL(
                    """
                insert into {work_schema}.{target_table} VALUES (
                {id},
                {epi_id},
                {epi_type},
                {date_raw},
                {date}, 
                {atc},
                {drug_name},
                {active_ingredient});
                """
                    ).format(
                        work_schema=sql.Identifier(self.work_schema),
                        target_table=sql.Identifier(str(self.prefix) + "_" + table_name),
                        id=sql.Literal(row[0]),
                        epi_id=sql.Literal(row[1]),
                        epi_type=sql.Literal(row[2]),
                        date_raw=sql.Literal(row[3]),
                        date=sql.Literal(new_date),
                        atc=sql.Literal(row[4]),
                        drug_name=sql.Literal(row[5]),
                        active_ingredient=sql.Literal(row[6])
                    )
            )

        conn.commit()
        cur.close()
        conn.close()

        self.mark_as_complete()
