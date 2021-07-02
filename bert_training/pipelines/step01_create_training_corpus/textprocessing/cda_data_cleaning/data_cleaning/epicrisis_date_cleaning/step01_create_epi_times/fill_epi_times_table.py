import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class FillEpiTimesTable(CDASubtask):

    config_file = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()
    prefix = luigi.Parameter()

    def run(self):
        self.log_current_action("Fill epi_times table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            INSERT INTO {target_schema}.{prefix}_epi_times 
            (
                epi_id, 
                epi_type, 
                reporting_time_raw, 
                effective_time_low_dt_raw, 
                effective_time_high_dt_raw, 
                first_visit_raw, 
                last_visit_raw
            ) 
            SELECT 
                {target_schema}.{prefix}_patient_raw.epi_id, 
                {target_schema}.{prefix}_patient_raw.epi_type_raw, 
                {target_schema}.{prefix}_patient_raw.epi_time_dt_raw, 
                {target_schema}.{prefix}_patient_raw.effective_time_low_dt_raw, 
                {target_schema}.{prefix}_patient_raw.effective_time_high_dt_raw, 
                select_union.first, 
                select_union.last 
            FROM {target_schema}.{prefix}_patient_raw 
                LEFT OUTER JOIN (
                SELECT 
                    {target_schema}.{prefix}_ambulatory_case_raw.epi_id AS epi_id, 
                    min({target_schema}.{prefix}_ambulatory_case_raw.effective_time_dt_raw) AS first, 
                    max({target_schema}.{prefix}_ambulatory_case_raw.effective_time_dt_raw) AS last 
                FROM {target_schema}.{prefix}_ambulatory_case_raw 
                GROUP BY {target_schema}.{prefix}_ambulatory_case_raw.epi_id 
                    UNION 
                SELECT 
                    {target_schema}.{prefix}_department_stay_raw.epi_id AS epi_id, 
                    min({target_schema}.{prefix}_department_stay_raw.start_time_dt_raw) AS first, 
                    max({target_schema}.{prefix}_department_stay_raw.end_time_dt_raw) AS last 
                FROM {target_schema}.{prefix}_department_stay_raw 
                GROUP BY {target_schema}.{prefix}_department_stay_raw.epi_id) AS select_union 
                    ON select_union.epi_id = {target_schema}.{prefix}_patient_raw.epi_id 
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                prefix=sql.SQL(self.prefix)
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
