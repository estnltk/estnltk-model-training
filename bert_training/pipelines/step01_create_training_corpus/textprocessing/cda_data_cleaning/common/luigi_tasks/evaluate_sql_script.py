import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDATask


class EvaluateSQLScript(CDATask):
    """
    A task for loading and evaluating SQL script.
    Opens a psql file, replaces all the parameters with values from **kwargs and executes the code.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    psql_file = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")

    def __init__(self, *args, **kwargs):
        # args- arguments without keyword (config file etc)
        # kwargs- arguments with keyword (possible units etc)
        super().__init__(*args, **kwargs)
        # TODO: Store non-reserved kayword arguments and use it as inputs to plsql script
        self.kwargs = kwargs

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        with open(str(self.psql_file)) as sql_string:
            print(sql_string)
            cur.execute(sql.SQL(sql_string.read().format(**self.kwargs)))
            conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
