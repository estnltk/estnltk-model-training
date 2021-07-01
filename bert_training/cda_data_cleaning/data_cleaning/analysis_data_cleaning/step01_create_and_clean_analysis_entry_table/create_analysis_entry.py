import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class CreateAnalysisEntryTable(CDASubtask):
    """
    By default imports analysis_entry table from the original to the work schema.
    It can be used for other tables that have the same structure as analysis_entry.

    New xml parsers gives a different format for analysis_entry table (way more columns).
    It is determine, if the structure comes from old or new parser and
    based on that it is decided how to proceed.

    However the end tables should have the same columns in both cases.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter(default="analysis_entry")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="analysis_entry")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        conn = self.create_postgres_connection(self.config_file)
        is_new_xml_parser = self.db_table_column_exists(
            conn, self.source_schema, self.source_table, "value_low_inclusive"
        )
        conn.close()

        if is_new_xml_parser:
            # new structure
            task = CreateAnalysisEntryTable2(
                config_file=self.config_file,
                role=self.role,
                source_schema=self.source_schema,
                source_table=self.source_table,
                target_schema=self.target_schema,
                target_table=self.target_table,
                luigi_targets_folder=self.luigi_targets_folder,
                requirement=self.requirement,
            )
        else:
            # old structure
            task = CreateAnalysisEntryTable1(
                config_file=self.config_file,
                role=self.role,
                source_schema=self.source_schema,
                source_table=self.source_table,
                target_schema=self.target_schema,
                target_table=self.target_table,
                luigi_targets_folder=self.luigi_targets_folder,
                requirement=self.requirement,
            )

        return [task]

    def run(self):
        self.log_current_time("Create analysis entry finishing time")
        self.mark_as_complete()


class CreateAnalysisEntryTable1(CDASubtask):
    """
    Table has the structure given by the old xml parser.
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
        self.log_current_action("Create analysis entry table old structure")
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
            from {source_schema}.{source_table} limit 100
            """
            ).format(
                source_schema=sql.Identifier(self.source_schema),
                source_table=sql.Identifier(self.source_table),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class CreateAnalysisEntryTable2(CDASubtask):
    """
    This step is for entry table with new structure produced by the new xml.
    Main difference is that some columns (value, reference_values) are not represented only in one column but many.

    1. Empty table is created.
    2. Useful columns from original.analysis_entry are imported.
    3. Some 'analysis_names' are moved under 'parameter_name' based on 'code_system_name'
    4. 8  different value columns are merged together under 'value_raw' column.
        value_raw,
        value_low_raw,
        value_low_inclusive_raw,
        value_high_raw,
        value_high_inclusive_raw,
        value_numerator_raw,
        value_denominator_raw,
        value_dispaly_name_raw
                ||
                V
            value_raw
    5. 5 different reference_value columns are merged together under 'reference_values_raw' column.
        reference_low_raw,
        reference_low_inclusive_raw,
        reference_high_raw,
        reference_high_inclusive_raw,
        reference_code_raw
            ||
            V
        reference_value_raw
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
        self.log_current_action("Create analysis entry table new structure")
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
                ---
                value_raw varchar,
                value_low_raw varchar,
                value_low_inclusive_raw varchar,
                value_high_raw varchar,
                value_high_inclusive_raw varchar,
                value_numerator_raw varchar,
                value_denominator_raw varchar,
                value_display_name_raw varchar,
                ---
                reference_low_raw varchar,
                reference_low_inclusive_raw varchar,
                reference_high_raw varchar,
                reference_high_inclusive_raw varchar,
                reference_code_raw varchar
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
                ---
                value_raw,
                value_low_raw,
                value_low_inclusive_raw,
                value_high_raw,
                value_high_inclusive_raw,
                value_numerator_raw,
                value_denominator_raw,
                value_display_name_raw,
                ---
                reference_low_raw,
                reference_low_inclusive_raw,
                reference_high_raw,
                reference_high_inclusive_raw,
                reference_code_raw
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
                ----
                value as value_raw,
                value_low as value_low_raw,
                value_low_inclusive as value_low_inclusive_raw,
                value_high as value_high_raw,
                value_high_inclusive as value_high_inclusive_raw,
                value_numerator as value_numerator_raw,
                value_denominator as value_denominator_raw,
                value_display_name as value_display_name_raw,
                ----
                reference_low as reference_low_raw,
                reference_low_inclusive as reference_low_inclusive_raw,
                reference_low_inclusive as reference_high_raw,
                reference_high_inclusive as reference_high_inclusive_raw,
                reference_code as reference_code_raw        
                        
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
                target_schema=sql.Identifier(self.target_schema), target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        # new structure has extra columns
        # in order to match entry and html, the value column must be presented in the same format
        # therefore, merge all the value columns into one column 'value_raw'
        cur.execute(
            sql.SQL(
                """
                update {target_schema}.{target_table}
                set value_raw = case
                        when value_raw is not null then value_raw
                        when value_low_raw is not null and value_low_inclusive_raw = 'false' then concat('(', value_low_raw, ';Inf)')
                        when value_low_raw is not null and value_low_inclusive_raw = 'true' then concat('[', value_low_raw, ';Inf)')
                        when value_high_raw is not null and value_high_inclusive_raw = 'false' then concat('(-Inf;', value_high_raw, ')')
                        when value_high_raw is not null and value_high_inclusive_raw = 'true' then concat('(-Inf;', value_high_raw, ']')
                        when value_numerator_raw is not null and value_denominator_raw is not null then concat(value_numerator_raw, '/', value_denominator_raw)
                        when value_display_name_raw is not null then value_display_name_raw
                    end;

                -- the reason for deleting the columns is that both old and new xml entry tables need to have the same
                -- structure at this step, so there are two possibilities
                -- 1. add empty columns to old structure
                -- 2. remove columns from new structure  (this is currently chosen)
                alter table {target_schema}.{target_table}
                    drop column value_low_raw,
                    drop column value_low_inclusive_raw,
                    drop column value_high_raw,
                    drop column value_high_inclusive_raw,
                    drop column value_numerator_raw,
                    drop column value_denominator_raw,
                    drop column value_display_name_raw;
                """
            ).format(
                target_schema=sql.Identifier(self.target_schema), target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        # in order to preserve the same structure for entry as html has,
        # there can only be one reference_values_raw column (new structure has ~10 related columns)
        # therefore, merge all the reference value related columns into one column 'reference_values_raw'
        cur.execute(
            sql.SQL(
                """
                update  {target_schema}.{target_table}
                set reference_values_raw = case
                                -- whole range is known
                                when reference_low_raw is not null and reference_high_raw is not null
                                    then case when reference_low_inclusive_raw = 'true' and reference_high_inclusive_raw = 'true'
                                                    then concat('[', reference_low_raw, ';',reference_high_raw,']')
                                              when reference_low_inclusive_raw = 'true' and reference_high_inclusive_raw = 'false'
                                                    then concat('[', reference_low_raw, ';',reference_high_raw,')')
                                              when reference_low_inclusive_raw = 'false' and reference_high_inclusive_raw = 'true'
                                                    then concat('(', reference_low_raw, ';',reference_high_raw,']')
                                              when reference_low_inclusive_raw = 'false' and reference_high_inclusive_raw = 'false'
                                                    then concat('(', reference_low_raw, ';',reference_high_raw,')')
                                        end
                                -- lower range is known
                                when reference_low_raw is not null and reference_low_inclusive_raw = 'false' then concat('(', reference_low_raw, ';Inf)')
                                when reference_low_raw is not null and reference_low_inclusive_raw = 'true' then concat('[', reference_low_raw, ';Inf)')
                                -- upper range is known
                                when reference_high_raw is not null and reference_high_inclusive_raw = 'false' then concat('(-Inf;', reference_high_raw, ')')
                                when reference_high_raw is not null and reference_high_inclusive_raw = 'true' then concat('(-Inf;', reference_high_raw, ']')
                                when reference_code_raw is not null then reference_code_raw
                                --when value_numerator is not null and value_denominator is not null then concat(value_numerator, '/', value_denominator)
                end
                where reference_values_raw is null;

                -- the reason for deleting the columns is that both old and new xml entry tables need to have the same
                -- structure at this step, so there are two possibilities
                -- 1. add empty columns to old structure
                -- 2. remove columns from new structure  (this is currently chosen)
                alter table {target_schema}.{target_table}
                    drop column reference_low_raw,
                    drop column reference_low_inclusive_raw,
                    drop column reference_high_raw,
                    drop column reference_high_inclusive_raw,
                    drop column reference_code_raw;
                """
            ).format(
                target_schema=sql.Identifier(self.target_schema), target_table=sql.Identifier(self.target_table),
            )
        )

        conn.commit()

        # now finally the tables has the same columns as old structure
        self.log_current_time("Create analysis entry finishing time")

        cur.close()
        conn.close()
        self.mark_as_complete()
