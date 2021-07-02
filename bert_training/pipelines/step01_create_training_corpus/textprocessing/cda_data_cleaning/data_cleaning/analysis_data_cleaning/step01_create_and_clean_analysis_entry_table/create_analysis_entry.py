import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class CreateAnalysisEntryTable(CDASubtask):
    """
    By default imports analysis_entry table from the original to the work schema.
    It can be used for other tables that have the same structure as analysis_entry.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter(default="analysis_entry")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="analysis_entry")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Create analysis entry table")
        self.log_schemas()
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
                id bigserial,
                ---
                original_analysis_entry_id bigint,
                epi_id varchar,
                epi_type varchar,
                analysis_id bigint,
                code_system_raw varchar,
                code_system_name_raw varchar,
                ---
                analysis_code_raw varchar,
                analysis_name_raw varchar,
                parameter_code_raw varchar,
                parameter_name_raw varchar,
                parameter_unit_raw varchar,
                reference_values_raw varchar,
                effective_time_raw varchar,
                value_raw varchar
            );
            reset role;
            """
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        # Update table in the standard role or we might get permission errors
        cur.execute(
            sql.SQL(
                """
            insert into {target_schema}.{target_table}
            (
                original_analysis_entry_id,
                ---
                epi_id,
                epi_type,
                analysis_id,
                code_system_raw,
                code_system_name_raw,
                ---
                analysis_code_raw,
                analysis_name_raw,
                parameter_code_raw,
                parameter_name_raw,
                parameter_unit_raw,
                reference_values_raw,
                effective_time_raw,
                value_raw
            )
            select 
                id as original_analysis_entry_id,
                ---
                epi_id, 
                epi_type, 
                analysis_id, 
                code_system as code_system_raw, 
                code_system as code_system_name_raw,
                ---
                analysis_code as analysis_code_raw,
                analysis_name as analysis_name_raw,
                parameter_code as parameter_code_raw,
                parameter_name as parameter_name_raw,
                parameter_unit as parameter_unit_raw,
                reference_values as reference_values_raw,
                effective_time as effective_time_raw,
                value as value_raw                              
            from {source_schema}.{source_table} 
            """
            ).format(
                source_schema=sql.Identifier(self.source_schema),
                source_table=sql.Identifier(self.source_table),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        # original.analysis_entry has values under wrong columns
        # main issue is that parameter_name_raw is placed under analysis_name_raw/analysis_code_raw column
        # based on "code_system_name" are values moved under right columns in the target table
        # other option would be creating a new table for modified column values but it seems unnecessary
        cur.execute(
            sql.SQL(
                """
            -- code_system LOINC
            update {target_schema}.{target_table}
            set 
                parameter_name_raw = analysis_name_raw,
                analysis_name_raw = NULL
            where 
                lower(code_system_name_raw) = 'loinc' and
                parameter_name_raw is null and
                analysis_name_raw not similar to 
                (
                    '%Hemogramm%|CBC-5Diff Hemogramm viieosalise leukogrammiga|Happe-aluse tasakaal%|' ||
                    'Uriini ribaanalüüs%|Uriini sademe mikroskoopia%|Hemogramm 5-osalise leukogrammiga%|' ||
                    'Vere automaatuuring 5-osalise leukogrammiga%|Vere automaatuuring ilma leukogrammita|' ||
                    'Vereäige mikroskoopia%|%vere automaatuuring|%paneel%'
                );

            -- code_system WATSON
            update {target_schema}.{target_table}
            set 
                parameter_name_raw = analysis_name_raw,
                analysis_name_raw = NULL
            where lower(code_system_name_raw) = 'watson';

            -- code_system ESTER ANALÜÜS
            update {target_schema}.{target_table}
            set 
                parameter_name_raw = analysis_code_raw,
                analysis_code_raw = NULL
            where lower(code_system_name_raw) = 'ester analüüs';
            """
            ).format(
                schema=sql.Identifier(self.target_schema),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
