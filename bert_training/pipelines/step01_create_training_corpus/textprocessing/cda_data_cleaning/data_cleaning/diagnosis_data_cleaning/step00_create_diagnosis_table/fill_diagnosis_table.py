import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class FillDiagnosisTable(CDASubtask):

    config_file = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="diagnosis")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Fill Diagnosis table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Filling from 'main_diagnosis'
        cur.execute(
            sql.SQL(
                """
            INSERT INTO {target_schema}.{target_table}
            (
                src_id, 
                epi_id, 
                epi_type, 
                diag_medical_type, 
                diag_code_raw, 
                diag_name_raw, 
                diag_text_raw, 
                diag_statistical_type_raw
            ) 
            SELECT 
                a.id, 
                a.epi_id, 
                a.epi_type, 
                'main' AS anon_1, 
                a.main_diag_code, 
                a.main_diag_name, 
                a.main_diag_text, 
                a.main_diag_statistical_type
            FROM {source_schema}.main_diagnosis a;
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                source_schema=sql.Identifier(self.source_schema)
            )
        )
        conn.commit()

        # Filling from 'complication_diagnosis'
        cur.execute(
            sql.SQL(
                """
            INSERT INTO {target_schema}.{target_table}
            (
                src_id, 
                epi_id, 
                epi_type, 
                diag_medical_type, 
                diag_code_raw, 
                diag_name_raw, 
                diag_text_raw
            ) 
            SELECT 
                a.id, 
                a.epi_id, 
                a.epi_type, 
                'compl' AS anon_1, 
                a.compl_diag_code, 
                a.compl_diag_name, 
                a.compl_diag_text
            FROM {source_schema}.complication_diagnosis a;
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                source_schema=sql.Identifier(self.source_schema)
            )
        )
        conn.commit()

        # Filling from 'by_illness_diagnosis'
        cur.execute(
            sql.SQL(
                """
            INSERT INTO {target_schema}.{target_table}
            (
                src_id, 
                epi_id, 
                epi_type, 
                diag_medical_type, 
                diag_code_raw, 
                diag_name_raw, 
                diag_text_raw
            ) 
            SELECT 
                a.id, 
                a.epi_id, 
                a.epi_type, 
                'by_illness' AS anon_1, 
                a.by_illness_diag_code, 
                a.by_illness_diag_name, 
                a.by_illness_diag_text
            FROM {source_schema}.by_illness_diagnosis a;
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                source_schema=sql.Identifier(self.source_schema)
            )
        )
        conn.commit()

        # Filling from 'outer_cause'
        cur.execute(
            sql.SQL(
                """
            INSERT INTO {target_schema}.{target_table}
            (
                src_id, 
                epi_id, 
                epi_type, 
                diag_medical_type, 
                diag_code_raw, 
                diag_name_raw, 
                diag_text_raw
            ) 
            SELECT 
                a.id, 
                a.epi_id, 
                a.epi_type, 
                'outer_cause' AS anon_1, 
                a.outer_cause_diag_code, 
                a.outer_cause_diag_name, 
                a.outer_cause_diag_text
            FROM {source_schema}.outer_cause_diagnosis a;
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                source_schema=sql.Identifier(self.source_schema)
            )
        )
        conn.commit()

        # Filling from 'other_diagnosis'
        cur.execute(
            sql.SQL(
                """
            INSERT INTO {target_schema}.{target_table}
            (
                src_id, 
                epi_id, 
                epi_type, 
                diag_medical_type, 
                diag_code_raw, 
                diag_name_raw, 
                diag_text_raw, 
                diag_statistical_type_raw
            ) 
            SELECT 
                a.id, 
                a.epi_id, 
                a.epi_type, 
                'other' AS anon_1, 
                a.other_diag_code, 
                a.other_diag_name, 
                a.other_diag_text, 
                a.other_diag_statistical_type
            FROM {source_schema}.other_diagnosis a;
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                source_schema=sql.Identifier(self.source_schema)
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
