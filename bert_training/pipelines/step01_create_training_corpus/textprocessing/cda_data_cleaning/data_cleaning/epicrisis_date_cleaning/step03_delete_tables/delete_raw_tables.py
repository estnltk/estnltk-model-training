import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class DeleteRawEpiTables(CDASubtask):

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    prefix = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Delete raw ambulatory, department stay and patient tables")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        table_names = [
            "_ambulatory_case_raw",
            "_patient_raw",
            "_department_stay_raw"
        ]

        for table in table_names:
            table_name = str(self.prefix) + table
            query = sql.SQL(
                """
                set role {role};
                drop table if exists {schema}.{table_name};
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                table_name=sql.Identifier(table_name)
            )

            cur.execute(query)
            conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
