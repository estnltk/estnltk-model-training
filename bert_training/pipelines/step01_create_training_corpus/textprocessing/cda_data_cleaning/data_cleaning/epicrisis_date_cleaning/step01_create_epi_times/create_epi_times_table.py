import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class CreateEpiTimesTable(CDASubtask):

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="epi_times")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Create epi_times table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Create empty table in the right role to guarantee access rights
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {target_schema}.{target_table};
            create table {target_schema}.{target_table}
            (
                id serial primary key,
                epi_id text,
                epi_type varchar,
                reporting_time_raw timestamp without time zone,
                effective_time_low_dt_raw timestamp without time zone,
                effective_time_high_dt_raw timestamp without time zone,
                first_visit_raw timestamp without time zone,
                last_visit_raw timestamp without time zone,
                reporting_time timestamp without time zone,
                epi_start timestamp without time zone,
                treatment_start timestamp without time zone,
                treatment_end timestamp without time zone,
                epi_end timestamp without time zone,
                applyed_cleaning varchar
            );
            reset role;"""
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
