import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class AddUnitsToPossibleUnitsTable(CDASubtask):
    """
    Inserts to table <prefix>_possible_units all distinct parameter_unit_raw values from analysis_html table.

    In the cleaning step the table is used to clean column value_raw, because in some cases value_raw contains both
    value and unit (e.g. 49 U/l).
    With the help of possible_units, value_raw column (49 U/l) can be split into value (49) and parameter_unit (U/l).

    Note: this step can not be done in step00_create_cleaning_functions because it requires the existence of
    analysis_html table.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="analysis_html")
    target_table = luigi.Parameter(default="possible_units")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Adding possible units")
        self.log_schemas()

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                """
            set role {role};
            insert into {schema}.{target}(unit)
            select distinct parameter_unit_raw as unit 
            from {schema}.{source}
            where parameter_unit_raw not in (select unit from {schema}.{target}) and parameter_unit_raw is not null;
            """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source=sql.Identifier(self.source_table),
                target=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
