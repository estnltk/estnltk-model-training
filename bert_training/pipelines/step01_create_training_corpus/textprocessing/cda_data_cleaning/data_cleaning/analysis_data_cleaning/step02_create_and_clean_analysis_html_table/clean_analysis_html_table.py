import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class CleanAnalysisHtmlTable(CDASubtask):
    """
    1. Checks if source_table has columns 'analysis_name_raw', 'parameter_name_raw',  'effective_time_raw',
        'reference_values_raw' and 'value_raw'
    2. Creates cleaning functions for 'analysis_name','parameter_name','effective_time','reference_values' and 'value'
    3. Cleans columns 'analysis_name_raw', 'parameter_name_raw', 'effective_time' and 'value'
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="analysis_html")
    target_table = luigi.Parameter(default="analysis_html_cleaned")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Clean analysis html table")
        self.log_schemas()
        # self.log_dependencises("Working with tables {source} and {target} \n".format(
        #     source=self.source_table, target=self.target_table))
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """   
            set role {role};                            
            set search_path to {schema};
            
            -- rows where effective_time_raw and value_raw are missing 
            -- do not contain any useful information and can be discarded            
            delete from {source}
            where value_raw is null or value_raw = '' or
                  (effective_time_raw is null and value_raw is null) or 
                  (effective_time_raw = '' and value_raw is null);

            set role {role};
            drop table if exists {target};
            create table {target} as
            select html.*,
                  {schema}.clean_analysis_name(analysis_name_raw) as analysis_name,
                  {schema}.clean_parameter_name(parameter_name_raw) as parameter_name,
                  {schema}.clean_effective_time(effective_time_raw) as effective_time,
                  unnest(cleaned_value_arrays[1]::text[]) as value, -- value_type='time_series' will be split into multiple rows, non-time series stays on the same row
                  cleaned_value_arrays[2] as parameter_unit_from_suffix,
                  cleaned_value_arrays[3] as suffix,
                  cleaned_value_arrays[4] as value_type,
                  -- initially time series are on one row (e.g. 6,5(08:52),6,5(08:52),)
                  -- during cleaning they are split to seperate rows
                  -- time_series_block show which rows where initially together, NULL for not time_series values
                  case
                       when cleaned_value_arrays[4] = 'time_series' then 
                           row_number() over (partition by epi_id, (case when cleaned_value_arrays[4]  = 'time_series' then 1 else 0 end)) 
                       end as time_series_block,
                  {schema}.clean_reference_value(reference_values_raw) as reference_values
            from {schema}.{source} as html,
                 {schema}.clean_values(value_raw, parameter_unit_raw) as cleaned_value_arrays;
            
            -- do not need column cleaned value
            alter table {target}
                drop column if exists cleaned_value;
            
            -- add column specifying the origin of the data row (html)    
            alter table {target}
                add column if not exists source varchar NOT NULL DEFAULT 'html';
               
            -- add unique id (can not do earlier, because time series values create multiple rows out of one row)
            UPDATE {target} t1
            SET id = t2.rownum
            FROM (SELECT ctid, row_number() OVER (order by ctid) as rownum
                FROM {target}) as t2
                WHERE t1.ctid = t2.ctid
            """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                target=sql.Identifier(self.target_table),
                source=sql.Identifier(self.source_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
