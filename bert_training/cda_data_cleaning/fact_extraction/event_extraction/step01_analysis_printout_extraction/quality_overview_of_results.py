import os
import csv
import luigi
from psycopg2 import sql
from cda_data_cleaning.common.luigi_tasks import CDASubtask


class QualityOverviewOfResults(CDASubtask):
    """
    Assumptions: analysis_texts_structured_cleaned exists
    Gives an overview of the content of analysis_texts_structured_cleaned and writes it into file:
        prefix + quality_overview_analysis_texts_structured_cleaned.csv
    """

    config_file = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    sourcetable = luigi.Parameter()
    output_filename = luigi.Parameter("quality_overview_analysis_texts_structured_cleaned")
    luigi_targets_folder = luigi.Parameter(default=".")

    def run(self):
        self.log_current_action("Quality overview to " + self.output_filename + ".csv")
        quality_check_file = os.path.join(
            os.path.dirname(__file__), "data/mistake_counter_analysis_texts_structured_cleaned.psql"
        )

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        with open(quality_check_file) as sql_string:
            cur.execute(
                sql.SQL(sql_string.read()).format(
                    schema=sql.Identifier(self.schema), table=sql.Identifier(self.sourcetable)
                )
            )
        results = cur.fetchall()

        filename = os.path.join(os.path.dirname(__file__), self.output_filename + ".csv")
        columns = [column[0] for column in cur.description]

        with open(filename, "w") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in results:
                writer.writerow(row)

        conn.commit()
        conn.close()
        self.mark_as_complete()
