import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import read_config
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import create_connection


class Diagnoses(CDASubtask):

    # none of those luigi.Parameter() are actually mandatory
    prefix = luigi.Parameter()
    config_file = luigi.Parameter()  # ex egcut_epi_microrun.ini, hwisc_epi_microrun.ini
    source_table = luigi.Parameter()  # input table name
    target_table = luigi.Parameter()  # output table name

    def requires(self):
        return []  # if depends on other classes, write them here

    def run(self):
        config = read_config(str(self.config_file))
        schema = config["database-configuration"]["work_schema"]
        role = config["database-configuration"]["role"]

        conn = create_connection(config)

        # creates table to work schema where main diagnosis name is different from icd10 canonical diagnosis name
        query = sql.SQL(
            """
            drop table if exists {schema_work}.{target_table};
        
            create table {schema_work}.{target_table} as
            select *
            from {schema_original}.{source_table} as md
                     -- canonical diangonis names from ehif_ins_bills classification
                     left join work.diagnoses_canonical_202002141010 cd
                               on md.main_diag_code = cd.diag_code
            where lower(md.main_diag_name) != lower(cd.icd10_name);
            """
        ).format(
            schema_work=sql.Identifier(schema),
            schema_original=sql.Identifier("original"),
            target_table=sql.Identifier(self.target_table),
            source_table=sql.Identifier(self.source_table),
        )

        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        conn.close()

        self.mark_as_complete()
