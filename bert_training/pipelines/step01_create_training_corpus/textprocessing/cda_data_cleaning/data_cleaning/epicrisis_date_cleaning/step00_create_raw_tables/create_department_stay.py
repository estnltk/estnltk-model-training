import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class CreateDepartmentStayTable(CDASubtask):

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter(default="department_stay")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="department_stay_raw")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Create Department Stay table")
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
                start_time_raw TEXT, 
                end_time_raw TEXT, 
                length_of_stay_raw TEXT, 
                performer_code_raw TEXT, 
                performer_name_raw TEXT, 
                start_time_dt_raw TIMESTAMP WITHOUT TIME ZONE, 
                end_time_dt_raw TIMESTAMP WITHOUT TIME ZONE, 
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
                start_time_raw, 
                end_time_raw, 
                length_of_stay_raw, 
                performer_code_raw, 
                performer_name_raw,
                start_time_dt_raw,
                end_time_dt_raw
            )
            select 
                id, 
                epi_id, 
                epi_type, 
                start_time, 
                end_time, 
                length_of_stay, 
                performer_code, 
                performer_name,
                {target_schema}.clean_effective_time(a.start_time::text),
                {target_schema}.clean_effective_time(a.end_time::text)
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
