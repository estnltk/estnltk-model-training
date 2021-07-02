import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class CreateAmbulatoryCaseTable(CDASubtask):

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter(default="ambulatory_case")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="ambulatory_case_raw")
    luigi_targets_folder = luigi.Parameter(default=".")

    def run(self):
        self.log_current_action("Create Ambulatory Case table")
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
                epi_type_raw TEXT, 
                effective_time_raw TEXT, 
                priority_code_raw TEXT, 
                priority_display_name_raw TEXT, 
                type_code_raw TEXT, 
                type_display_name_raw TEXT, 
                kind_code_raw TEXT, 
                kind_display_name_raw TEXT, 
                effective_time_dt_raw TIMESTAMP WITHOUT TIME ZONE, 
                PRIMARY KEY (id)
            );
            reset role;
            """
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        # Update table in the standard role or we might get permission errors
        cur.execute(
            sql.SQL(
                """
            insert into {target_schema}.{target_table}
            (
                src_id, 
                epi_id, 
                epi_type_raw, 
                effective_time_raw, 
                priority_code_raw, 
                priority_display_name_raw, 
                type_code_raw, 
                type_display_name_raw, 
                kind_code_raw, 
                kind_display_name_raw,
                effective_time_dt_raw
            )
            select 
                id, 
                epi_id, 
                epi_type, 
                effective_time, 
                priority_code, 
                priority_display_name, 
                type_code, 
                type_display_name, 
                kind_code, 
                kind_display_name,
                {target_schema}.clean_effective_time(a.effective_time::text)
            from {source_schema}.{source_table} a; 
            """
            ).format(
                source_schema=sql.Identifier(self.source_schema),
                source_table=sql.Identifier(self.source_table),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
