import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class CreateAnalysisHtmlTable(CDASubtask):
    """
    Create empty tables
        - <prefix>_analysis_html
        - <prefix>_analysis_html_log
        - <prefix>_analysis_html_meta
    which will be filled in parse_analysis_html_table step.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="analysis_html")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Create Analysis Html Tables")
        self.log_schemas()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        table_name = str(self.target_table)
        table_name_log = table_name + "_log"
        table_name_meta = table_name + "_meta"

        # TODO: Define an appropriate log message function
        print(
            "Working with tables {tbl1}, {tbl2} and {tbl3} \n".format(
                tbl1=table_name, tbl2=table_name_log, tbl3=table_name_meta
            )
        )

        # Create analysis_html
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {schema}.{table};
            create table {schema}.{table} 
            (   row_nr int,
                epi_id varchar,
                epi_type varchar,
                parse_type varchar,
                panel_id int,
                analysis_name_raw varchar,
                parameter_name_raw varchar,
                parameter_unit_raw varchar,
                reference_values_raw varchar,
                effective_time_raw varchar,
                value_raw varchar,
                analysis_substrate_raw varchar,
                id integer -- initially empty, will be filled in cleaning step
            );
            reset role;
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                table=sql.Identifier(table_name),
            )
        )

        # Create analysis_html_log
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {schema}.{table};
            create table {schema}.{table} 
            (
                time varchar,
                epi_id varchar,
                type varchar,
                message varchar
            );
            reset role;
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                table=sql.Identifier(table_name_log),
            )
        )

        # Create analysis_html_meta
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {schema}.{table};
            create table {schema}.{table} (
                  epi_id varchar,
                  attribute varchar,
                  value varchar
            );
            reset role;
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                table=sql.Identifier(table_name_meta),
            )
        )

        conn.commit()
        conn.close()
        self.mark_as_complete()
