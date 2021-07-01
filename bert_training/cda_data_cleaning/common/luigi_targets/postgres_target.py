import luigi

class LuigiPostgresTarget(luigi.Target):
    """
    A target that is stored into a Postgres database table.

    This allows much cleaner ETL process as history is stored in a centralised location

    The code must be ported from

    https://github.com/spotify/luigi/blob/master/luigi/contrib/sqla.py

    In development. The intention is to save luigi targets in a PostgreSQL table.

    """

    def __init__(self, config_file, prefix, task_instance, task_id):
        self.config_file = config_file

    def exists(self):
        pass
