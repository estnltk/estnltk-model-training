import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDABatchTask

# export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# sourcetable="run_202101080024_analysis_texts_structured"
# targettable="run_202101080024_analysis_texts_structured_cleaned"
# conf="configurations/egcut_epi.ini"
# luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.analysis_data_cleaning.analysis_printout_extraction.development.clean_analysis_texts CleanAnalysisTextsTableDev --config=$conf --role=egcut_epi_work_create --schema=work --source-table=$sourcetable --target-table=$targettable --workers=1 --log-level=INFO


class CleanAnalysisPrintoutTableDev(CDABatchTask):
    """
    Assumptions: analysis_texts_structured table has columns 'analysis_name_raw', 'parameter_name_raw',
    'parameter_unit_raw', 'effective_time_raw' and 'value_raw'

    Creates columns 'analysis_name', 'parameter_name', 'effective_time' and 'value' which contain cleaned values
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="analysis_entry")
    target_table = luigi.Parameter(default="analysis_entry_cleaned")
    luigi_targets_folder = luigi.Parameter(default=".")

    def run(self):
        self.log_current_action("Clean analysis texts table")
        # self.log_dependencises("Working with tables {source} and {target} \n".format(
        #     source=self.source_table, target=self.target_table))

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

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
                  {schema}.clean_reference_value(reference_values_raw) as reference_values
            from {schema}.{source},
                 {schema}.clean_values(value_raw, parameter_unit_raw) as cleaned_value;
                      
            -- do not need column cleaned value
            alter table {target}
                drop column if exists cleaned_value;
            
            -- add column specifying the origin of the data row (text field)    
            alter table {target}
                add column if not exists source varchar NOT NULL DEFAULT 'texts';
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
        self.mark_as_complete()
