import os
import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob


class ExportTaggerMappingsFromCsv(CDAJob):

    config_file = luigi.Parameter()
    target_table = luigi.Parameter(default="diagnosis_taggers_mapping")
    luigi_targets_folder = luigi.Parameter(default="luigi_targets")
    prefix = luigi.Parameter()

    # input_csv = luigi.Parameter()
    # output_csv = luigi.Parameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action("Importing diagnosis code mapping table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()
        # grammar_symbol, regex_type, key
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {schema}.{table};
            create table {schema}.{table}
            (
                grammar_symbol varchar,
                regex_type varchar,
                "key" varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(self.work_schema),
                table=sql.Identifier(self.target_table),
                role=sql.Literal(self.role),
            )
        )
        conn.commit()

        csv_file = os.path.join(os.path.dirname(__file__), "tagger_mappings.csv")
        with open(csv_file, "r") as f:
            next(f)
            cur.copy_from(f, self.work_schema + "." + self.target_table, sep=",")

        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
