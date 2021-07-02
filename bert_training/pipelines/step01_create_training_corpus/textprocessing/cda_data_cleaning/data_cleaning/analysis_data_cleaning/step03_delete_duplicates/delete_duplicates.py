import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class DeleteDuplicates(CDASubtask):
    """
    Deletes duplicate rows from either entry or HTML table.

    TODO: Write a separate PSQL function for detectiong duplicates
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    html_source_table = luigi.Parameter(default="analysis_html_loinced")
    entry_source_table = luigi.Parameter(default="analysis_entry_loinced")
    html_target_table = luigi.Parameter(default="analysis_html_loinced_unique")
    entry_target_table = luigi.Parameter(default="analysis_entry_loinced_unique")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    source_tables = ["analysis_html_loinced", "analysis_entry_loinced"]
    target_tables = ["analysis_html_loinced_unique", "analysis_entry_loinced_unique"]

    def run(self):
        self.log_current_action("Deleting Duplicates")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # delete HTML duplicates
        cur.execute(
            sql.SQL(
                """
            set role {role};
            set search_path to {schema};
            
            drop table if exists {target};
            create table {target} as
            select * from
            (
                select *, row_number() over (partition by 
                (
                    epi_id,
                    epi_type,
                    parse_type,
                    panel_id,
                    analysis_name_raw,
                    parameter_name_raw,
                    parameter_unit_raw,
                    reference_values_raw,
                    value_raw,
                    analysis_substrate_raw,
                    --analysis_substrate,
                    --entry_match_id,
                    --entry_html_match_desc,
                    effective_time_raw,
                    analysis_name,
                    parameter_name,
                    effective_time,
                    value,
                    parameter_unit,
                    suffix,
                    value_type,
                    reference_values)
                    order by epi_id desc
                ) rn
            from {source}
        ) as tmp
        where rn = 1;
        
        -- do not need row number anymore
        alter table {target}
            drop column if exists rn;
        """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                target=sql.Identifier(self.html_target_table),
                source=sql.Identifier(self.html_source_table),
            )
        )
        conn.commit()

        # delete ENTRY duplicates
        cur.execute(
            sql.SQL(
                """
            set role {role};
            set search_path to {schema};
            
            drop table if exists {target};
            create table {target} as
            select * from
            (
                select *, row_number() over (partition by 
                (
                    epi_id,
                    epi_type,
                    --parse_type,
                    --panel_id,
                    code_system_raw, 
                    code_system_name_raw,
                    analysis_code_raw,
                    parameter_code_raw,
                    analysis_name_raw,
                    parameter_name_raw,
                    parameter_unit_raw,
                    reference_values_raw,
                    value_raw,
                    --analysis_substrate_raw,
                    --analysis_substrate,
                    --entry_match_id,
                    --entry_html_match_desc,
                    effective_time_raw,
                    analysis_name,
                    parameter_name,
                    effective_time,
                    value,
                    parameter_unit,
                    suffix,
                    value_type,
                    reference_values)
                    order by epi_id desc) rn
                from {source}
            ) as tmp
            where rn = 1;
            
            -- do not need row number anymore
            alter table {target}
                drop column if exists rn;
            """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                target=sql.Identifier(self.entry_target_table),
                source=sql.Identifier(self.entry_source_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
