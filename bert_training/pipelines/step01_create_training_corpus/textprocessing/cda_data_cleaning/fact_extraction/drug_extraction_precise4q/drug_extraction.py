import os
import re
import luigi

from psycopg2 import sql
from estnltk import Text
from tqdm import tqdm

from cda_data_cleaning.common.read_config import read_config
from cda_data_cleaning.common.luigi_targets import luigi_targets_folder
from cda_data_cleaning.common.db_operations import create_connection

from .taggers.freetext_drug_tagger import Precise4qDrugNameTagger
from .taggers.statin_tagger import StatinTagger


class ExtractPrecise4qDrugEntries(luigi.Task):
    """
    Extracts drug entries defined in Precise4q study from the following free text fields:
    - anamnesis.anamsum
    - anamnesis.anamnesis
    - anamnesis.diagnosis
    - anamnesis.dcase
    - summary.sumsum
    - summary.drug

    Tries to ignore drug entries that are used in context of drug allergies, e.g.
    patient is allergic to penicillin. Nothing fancy but should work.
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()

    def requires(self):
        return []

    def run(self):
        config = read_config(str(self.config_file))
        role = sql.Identifier(config["database-configuration"]["role"])
        schema = sql.Identifier(config["database-configuration"]["work_schema"])
        table = sql.Identifier(self.prefix + "_drug_free_text" if len(self.prefix) else "drug_free_text")

        print(
            "--------------------------Extracting Precise4q drug entries--------------------------------------------"
        )
        print("* Collecting texts to analyze")

        conn = create_connection(config)
        cur = conn.cursor()

        # Lets collect all text to be tagged into a big list.
        # Good enough for now but must be reworked later on.
        epis_and_texts = []

        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            SELECT id, epi_id, anamsum, anamnesis, diagnosis, dcase FROM original.anamnesis;
            """
            ).format(role=role)
        )
        for row in cur.fetchall():
            epis_and_texts.append([row[2], row[1], "original.anamnesis", "anamsum", row[0]])
            epis_and_texts.append([row[3], row[1], "original.anamnesis", "anamnesis", row[0]])
            epis_and_texts.append([row[4], row[1], "original.anamnesis", "diagnosis", row[0]])
            epis_and_texts.append([row[5], row[1], "original.anamnesis", "dcase", row[0]])

        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            SELECT id, epi_id, sumsum, drug FROM original.summary;
            """
            ).format(role=role)
        )
        for row in cur.fetchall():
            epis_and_texts.append([row[2], row[1], "original.summary", "sumsum", row[0]])
            epis_and_texts.append([row[3], row[1], "original.summary", "drug", row[0]])

        print("* Extracting drug names")

        tagger = Precise4qDrugNameTagger(output_layer="drug_names")
        statin_tagger = StatinTagger(output_layer="statins")
        tagged_rows = []

        drug_allergy_re = re.compile(r"(tundl|allerg|resist)")
        for row in tqdm(epis_and_texts):

            # Ignore entiries that are unsed in the context of drug allergies
            if not row[0] or drug_allergy_re.search(row[0].lower()):
                continue

            text = Text(row[0])
            tagger.tag(text)
            statin_tagger.tag(text)
            if len(text.drug_names) > 0:
                for drug in text.drug_names:
                    tagged_rows.append([row[1], row[2], row[3], row[4], drug.text.strip(), ""])
            if len(text.statins) > 0:
                for s in text.statins:
                    tagged_rows.append([row[1], row[2], row[3], row[4], s.text.strip(), s.substance[0]])

        print(
            "* Creating table {}".format(
                sql.SQL("{schema}.{table}").format(schema=schema, table=table).as_string(conn)
            )
        )

        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            DROP TABLE if exists {schema}.{table};
            CREATE TABLE {schema}.{table}(
                epi_id text,
                table_name text,
                field_name text,
                field_id text,
                drug text,
                drug_normalised text
            );
            """
            ).format(role=role, schema=schema, table=table)
        )

        query = sql.SQL(
            """
            SET ROLE {role};
            INSERT INTO {schema}.{table} VALUES (%s, %s, %s, %s, %s, %s);
            """
        ).format(role=role, schema=schema, table=table)

        for row in tagged_rows:
            cur.execute(query, (row[0], row[1], row[2], row[3], row[4], row[5]))

        conn.commit()
        conn.close()

        print("* All done")
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "parse_precise4q_drugs"))
