import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class CleanAnalysisPrintoutTable(CDASubtask):
    """
    Assumptions: analysis_texts_structured table has columns 'analysis_name_raw', 'parameter_name_raw',
    'parameter_unit_raw', 'effective_time_raw' and 'value_raw'

    Creates columns 'analysis_name', 'parameter_name', 'effective_time' and 'value' which contain cleaned values
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="analysis_texts_structured")
    target_table = luigi.Parameter(default="analysis_texts_structured_cleaned")
    possible_units = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Clean analysis printout table")
        self.log_current_time("Clean analysis printout starting time")
        # self.log_dependencises("Working with tables {source} and {target} \n".format(
        #     source=self.source_table, target=self.target_table))

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                """
            set role {role};
            set search_path to {schema};
            
            drop table if exists {schema}.{target_temp};
            create table {schema}.{target_temp} as
                -- unify empty values
                select work.empty_value_to_null(analysis_name_raw)    as analysis_name_raw2,
                       work.empty_value_to_null(parameter_name_raw)   as parameter_name_raw2,
                       work.empty_value_to_null(effective_time_raw)   as effective_time_raw2,
                       work.empty_value_to_null(parameter_unit_raw)   as parameter_unit_raw2,
                       work.empty_value_to_null(reference_values_raw) as reference_values_raw2,
                       work.empty_value_to_null(value_raw)            as value_raw2,
                       *
                from {schema}.{source};
            reset role;
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.schema),
                target_temp=sql.Identifier(self.target_table + "_temp"),
                source=sql.Identifier(self.source_table),
            )
        )

        cur.execute(
            sql.SQL(
                """
            set role {role};
            set search_path to {schema};

            drop table if exists {schema}.{target};
            create table {schema}.{target} as
            -- clean columns
            select id,
                   epi_id,
                   work.clean_analysis_name(analysis_name_raw2)     as analysis_name,
                   work.clean_parameter_name(parameter_name_raw2)   as parameter_name,
                   parameter_unit_raw2                              as parameter_unit,
                   work.clean_effective_time(effective_time_raw2)   as effective_time,
                   unnest(cleaned_value[1]::text[])                 as value,
                   cleaned_value[2]                                 as parameter_unit_from_suffix,
                   cleaned_value[3]                                 as suffix,
                   cleaned_value[4]                                 as value_type,
                   work.clean_reference_value(reference_values_raw) as reference_values,
                   text_raw,
                   text_type,
                   source_schema,
                   source_table,
                   source_column,
                   source_table_id,
                   'printout'                                       as source,
                   analysis_name_raw,
                   parameter_name_raw,
                   parameter_unit_raw,
                   value_raw,
                   reference_values_raw
            from {schema}.{target_temp},
                 work.clean_values(value_raw2, parameter_unit_raw2) as cleaned_value;
            reset role;

            --remove rows were both parameter name and analysis name are missing
            delete from {schema}.{target}
                where (analysis_name_raw is null) and (parameter_name_raw is null);
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.schema),
                target=sql.Identifier(self.target_table),
                target_temp=sql.Identifier(self.target_table + "_temp"),
                source=sql.Identifier(self.source_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.log_current_time("Clean analysis printout ending time")
        self.mark_as_complete()
