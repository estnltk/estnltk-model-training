import luigi
from psycopg2 import sql
from cda_data_cleaning.common.luigi_tasks.cda_subtask import CDASubtask

# export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# targettable="run_202010171513_analysis_procedure"
# conf="configurations/egcut_epi_microrun.ini"
# luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.analysis_data_cleaning.step01_analysis_printout_extraction.create_analysis_text_field_table CreateAnalysisTextFieldTable --config=$conf --role=egcut_epi_work_create --target-table=$targettable --workers=1 --log-level=INFO


class CreateAnalysisPrintoutTables(CDASubtask):
    """
    Creates empty table where
    1) extract analysis texts tables from 'text' field can be placed
    2) structured analysis texts can be placed
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    target_table_texts = luigi.Parameter(default="analysis_texts")
    target_table_texts_struc = luigi.Parameter(default="analysis_texts_structured")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Create analysis text field table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
                set role {role};
                drop table if exists {schema}.{table};
                create table {schema}.{table} 
                (   epi_id text,
                    table_text text,
                    source_schema text,
                    source_table text,
                    source_column text,
                    source_table_id integer,
                    text_type text,
                    span_start integer,
                    span_end integer
                );
                reset role;
                """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                table=sql.Identifier(self.target_table_texts),
            )
        )

        cur.execute(
            sql.SQL(
                """
                set role {role};
                drop table if exists {schema}.{table};
                create table {schema}.{table}
                    (epi_id text,
                    analysis_name_raw text,
                    parameter_name_raw text,
                    value_raw text,
                    parameter_unit_raw text,
                    effective_time_raw text,
                    reference_values_raw text,
                    text_raw text,
                    text_type text,
                    source_schema text,
                    source_table text,
                    source_column text,
                    source_table_id integer
                );
                reset role;
                """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                table=sql.Identifier(self.target_table_texts_struc),
            )
        )

        conn.commit()

        cur.close()
        conn.close()

        self.mark_as_complete()
