import os
import csv
import luigi
from psycopg2 import sql
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class SummariseAnalysisCleaned(CDASubtask):
    """
    Assumptions: analysis_cleaned exists
    Gives a summary of the content of analysis_cleaned
        * writes it into file 'prefix + analysis_cleaned_summary.csv'
        * writes it into database 'prefix + analysis_cleaned_summary'
    """

    config_file = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    source_table = luigi.Parameter(default="analysis_cleaned")
    temp_table = luigi.Parameter(default="analysis_cleaned_summary_counts")
    target_table = luigi.Parameter(default="analysis_cleaned_summary")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Summarising analysis_cleaned to " + self.target_table)
        summary_sql_file = os.path.join(os.path.dirname(__file__), "summarise_analysis_cleaned.psql")

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # execute psql file, creates a table to database
        with open(summary_sql_file) as sql_string:
            cur.execute(
                sql.SQL(sql_string.read()).format(
                    schema=sql.Identifier(self.schema),
                    role=sql.Identifier(self.role),
                    source=sql.Identifier(self.source_table),
                    temp=sql.Identifier(self.temp_table),
                    target=sql.Identifier(self.target_table),
                )
            )

        results = cur.fetchall()

        target_filename = os.path.join(os.path.dirname(__file__), "summaries", self.target_table) + ".csv"
        columns = [column[0] for column in cur.description]

        # write the results into file
        with open(target_filename, "w") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in results:
                writer.writerow(row)

        conn.commit()
        conn.close()
        self.mark_as_complete()
