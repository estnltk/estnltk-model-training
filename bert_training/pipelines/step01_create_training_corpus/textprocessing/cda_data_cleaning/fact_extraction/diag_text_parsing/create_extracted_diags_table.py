import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDAJob


class CreateExtractedDiagsTable(CDAJob):

    config_file = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default="luigi_targets")

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action("Create parsed diagnosis table")
        self.log_schemas()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        target_table = self.prefix + "_" + "diagnosis_parsed"

        # Create empty table in the right role to guarantee access rights
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {target_schema}.{target_table};
            create table {target_schema}.{target_table}
            (
                date text,
                epi_start text,
                key text,
                value text,
                "table" text,
                field text,
                src_id bigint,
                epi_id text,
                span_start int,
                span_end int,
                collection text,
                text_id bigint
            );
            reset role;
            """
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.work_schema),
                target_table=sql.Identifier(target_table),
            )
        )
        conn.commit()

        # Update table in the standard role or we might get permission errors
        cur.execute(
            sql.SQL(
                """
            insert into {target_schema}.{target_table}
            (
                date,
                epi_start,
                key,
                value,
                "table",
                field,
                src_id,
                epi_id,
                span_start,
                span_end,
                collection,
                text_id
            )
            SELECT
                null as "date",
                {target_schema}.clean_effective_time(patient.effective_time_low) as "epi_start",
                c.key AS key,
                u.value,
                u.table,
                u.field,
                u.src_id,
                u.epi_id,
                u.span_start,
                u.span_end,
                u.collection,
                u.text_id
            FROM
                {original_schema}.patient patient,
            (
                SELECT
                    src_id::bigint,
                    'cancer_stage_tagger' AS "key",
                    value,
                    span_start,
                    span_end,
                    text_id,
                    grammar_symbol,
                    regex_type,
                    "table",
                    field,
                    collection,
                    epi_id
                FROM
                    {target_schema}.temp_{prefix}_diagnosis_parsing_cancer_stages
            UNION
                SELECT
                    src_id::bigint,
                    'dates_numbers_tagger' AS "key",
                    value,
                    span_start,
                    span_end,
                    text_id,
                    grammar_symbol,
                    regex_type,
                    "table",
                    field,
                    collection,
                    epi_id
                FROM
                    {target_schema}.temp_{prefix}_diagnosis_parsing_dates_numbers
            UNION
                SELECT
                    src_id::bigint,
                    'diagnosis_tagger' AS "key",
                    value,
                    span_start,
                    span_end,
                    text_id,
                    grammar_symbol,
                    regex_type,
                    "table",
                    field,
                    collection,
                    epi_id
                FROM
                    {target_schema}.temp_{prefix}_diagnosis_parsing_diagnosis
            UNION
                SELECT
                    src_id::bigint,
                    'stages_tagger' AS "key",
                    value,
                    span_start,
                    span_end,
                    text_id,
                    grammar_symbol,
                    regex_type,
                    "table",
                    field,
                    collection,
                    epi_id
                FROM
                    {target_schema}.temp_{prefix}_diagnosis_parsing_stages) u
            LEFT JOIN {target_schema}.diagnosis_taggers_mapping c
                ON
                    u.grammar_symbol = c.grammar_symbol AND u.regex_type=c.regex_type
            WHERE
                patient.epi_id = u.epi_id
            """
            ).format(
                target_schema=sql.Identifier(self.work_schema),
                original_schema=sql.Identifier(self.original_schema),
                target_table=sql.Identifier(target_table),
                prefix=sql.SQL(self.prefix)
            )
        )
        conn.commit()

        # Updating dates from tagger results
        cur.execute(
            sql.SQL(
                """
            set role {role};
            UPDATE 
                {target_schema}.{target_table}
            SET 
                "date" = COALESCE({target_schema}.clean_effective_time(value)::text, value)
            WHERE 
                key = 'kuupäev';

            UPDATE 
                {target_schema}.{target_table}
            SET 
                "date" = value
            WHERE 
                key = 'osaline kuupäev';
            reset role;
            """
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.work_schema),
                target_table=sql.Identifier(target_table),
            )
        )
        conn.commit()

        # Indexing epi_id
        cur.execute(
            sql.SQL(
                """
            CREATE INDEX if not exists idx_epi_id 
            ON {target_schema}.{target_table} (epi_id)
            """
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.work_schema),
                target_table=sql.Identifier(target_table),
            )
        )
        conn.commit()

        # Indexing src_id and text_id
        cur.execute(
            sql.SQL(
                """
            CREATE INDEX if not exists idx_src_text_ids
            ON {target_schema}.{target_table} (src_id, text_id)
            """
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.work_schema),
                target_table=sql.Identifier(target_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
