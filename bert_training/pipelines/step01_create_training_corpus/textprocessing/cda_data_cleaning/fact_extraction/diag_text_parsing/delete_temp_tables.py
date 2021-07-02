import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDAJob


class DeleteTempTables(CDAJob):

    config_file = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default="luigi_targets")

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action("Deleting temp tables")
        self.log_schemas()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        tables_to_delete = ['cancer_stages', 'stages', 'dates_numbers', 'diagnosis']

        # Delete the temp tables
        for table in tables_to_delete:
            table_to_delete = "temp_" + self.prefix + "_diagnosis_parsing_" + table
            cur.execute(
                sql.SQL(
                    """
                    set role {role};
                    drop table if exists {table};
                    reset role;
                """
                ).format(
                    role=sql.Literal(self.role),
                    table=sql.SQL(self.work_schema + "." + table_to_delete)
                )
            )
            conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
