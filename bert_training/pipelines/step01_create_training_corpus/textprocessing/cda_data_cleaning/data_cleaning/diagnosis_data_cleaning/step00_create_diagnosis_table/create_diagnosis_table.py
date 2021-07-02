import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDATask


class CreateDiagnosisTable(CDATask):

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="diagnosis")
    luigi_targets_folder = luigi.Parameter(default=".")

    def run(self):
        self.log_current_action("Create Diagnosis table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Create empty table in the right role to guarantee access rights
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {target_schema}.{target_table};
            create table {target_schema}.{target_table}
            (
                id SERIAL NOT NULL, 
                src_id INTEGER, 
                epi_id TEXT, 
                epi_type VARCHAR, 
                diag_medical_type VARCHAR, 
                diag_code_raw VARCHAR, 
                diag_name_raw VARCHAR, 
                diag_text_raw VARCHAR, 
                diag_statistical_type_raw VARCHAR, 
                diag_code VARCHAR, 
                diag_name VARCHAR, 
                diag_name_additional VARCHAR, 
                cleaning_state VARCHAR, 
                diag_statistical_type VARCHAR, 
                clean BOOLEAN, 
                CONSTRAINT {pkey} PRIMARY KEY (id)
            );
            reset role;
            """
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                pkey=sql.Identifier(self.target_table + "_pkey")
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
