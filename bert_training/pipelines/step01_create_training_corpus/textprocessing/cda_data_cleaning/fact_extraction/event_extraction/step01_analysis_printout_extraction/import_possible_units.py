import os
import luigi
from psycopg2 import sql

from cda_data_cleaning.common.db_operations import import_csv_file
from cda_data_cleaning.common.luigi_tasks import CDASubtask


class ImportPossibleUnitsTable(CDASubtask):
    """
    Creates a table <prefix>_possible_units which contains all distinct parameter_unit_raw values
    from analysis_entry table.

    In the cleaning step the table is used to clean column value_raw, because in some cases value_raw contains both
    value and unit (e.g. 49 U/l).
    With the help of possible_units, value_raw column (49 U/l) can be split into value (49) and parameter_unit (U/l).

    Note: this step can not be done in step00_create_cleaning_functions because it requires the existence of
    analysis_entry table.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="possible_units")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Importing possible units table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {schema}.{table};
            create table {schema}.{table}
            (
                unit varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier(self.target_table),
                role=sql.Literal(self.role),
            )
        )
        conn.commit()

        csv_file = os.path.join(os.path.dirname(__file__), "data/", "possible_units.csv")
        import_csv_file(conn, csv_file, self.schema, self.target_table)
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
