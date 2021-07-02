import luigi

from cda_data_cleaning.common.luigi_tasks import CDABatchTask


class ValidateAssumptions(CDABatchTask):
    """
    Cleaning code (step01_clean_entry and step02_parse_and_clean_drug_text_field) assume
    that there exists a 'classification' schema with a table 'active_substance' inside.
    This step check if the assumption is fulfilled and if not then an error messages is raised.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        if not self.db_schema_exists(conn=conn, schema="classifications"):
            raise Exception("\nSchema 'classifications' needs to exist!\n\n")
        elif not self.db_table_exists(conn=conn, table="active_substances", schema="classifications"):
            raise Exception(
                "\n\nTable 'active_substances' needs to exists in classifications schema, because it is needed for cleaning the tables\n\n"
            )

        cur.close()
        conn.close()
        self.mark_as_complete()
