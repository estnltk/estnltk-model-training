import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class CleanAnalysisEntryTable(CDASubtask):
    """
    Assumptions: analysis_entry table has columns 'analysis_name_raw', 'parameter_name_raw', 'reference_values_raw',
           'effective_time_raw' and 'value_raw'

    Creates columns 'analysis_name', 'parameter_name', 'effective_time', 'reference_value' and 'value'
    which contain cleaned values
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="analysis_entry")
    target_table = luigi.Parameter(default="analysis_entry_cleaned")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Clean analysis entry table")
        self.log_schemas()

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # applying cleaning functions
        cur.execute(
            sql.SQL(
                """
            set role {role};
            set search_path to {schema};
            drop table if exists {schema}.{target};
            create table {schema}.{target} as
            select *,
                  {schema}.clean_analysis_name(analysis_name_raw) as analysis_name,
                  {schema}.clean_parameter_name(parameter_name_raw) as parameter_name,
                  {schema}.clean_effective_time(effective_time_raw) as effective_time,
                  unnest(cleaned_value[1]::text[]) as value,
                  cleaned_value[2] as parameter_unit_from_suffix,
                  cleaned_value[3] as suffix,
                  cleaned_value[4] as value_type,
                  -- initially time series are on one row (e.g. 6,5(08:52),6,5(08:52),)
                  -- during cleaning they are split to seperate rows
                  -- time_series_block show which rows where initially together, NULL for not time_series values
                  case
                       when cleaned_value[4] = 'time_series' then 
                           row_number() over (partition by epi_id, (case when cleaned_value[4] = 'time_series' then 1 else 0 end)) 
                       end as time_series_block,
                  {schema}.clean_reference_value(reference_values_raw) as reference_values
            from {schema}.{source},
                 {schema}.clean_values(value_raw, parameter_unit_raw) as cleaned_value;
                      
            -- do not need column cleaned value
            alter table {target}
                drop column if exists cleaned_value;
            
            -- add column specifying the origin of the data row (entry)    
            alter table {target}
                add column if not exists source varchar NOT NULL DEFAULT 'entry';
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.schema),
                target=sql.Identifier(self.target_table),
                source=sql.Identifier(self.source_table),
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        self.log_current_time("Clean analysis entry finishing time")
        self.mark_as_complete()
