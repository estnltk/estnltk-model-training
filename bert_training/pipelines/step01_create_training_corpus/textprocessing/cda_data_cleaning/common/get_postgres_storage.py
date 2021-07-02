from contextlib import contextmanager
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import read_config
from estnltk.storage import PostgresStorage


@contextmanager
def get_postgres_storage(config_file: str):
    """
    :param config_file:
        name of the configuration file in the cda-data-cleanin/configurations folder that contains at least
        [database-configuration]
        host = ...
        port = ... error: Module not callable
        database_name = ...
        username = ...
        password = ...
        role = ...
        work_schema = ...

    :return: EstNLTK PostgresStorage object

    usage:
        from cda_data_cleaning.common import get_postgres_storage

        with get_postgres_storage(config_file='egcut_epi_microrun.ini') as storage:
            do stuff with storage

    TODO: Make it as a static method of CDATask. Make sure that this universal
    """
    storage = None
    try:
        db_config = read_config(config_file)["database-configuration"]
        storage = PostgresStorage(
            dbname=db_config["database_name"],
            user=db_config["username"],
            password=db_config["password"],
            schema=db_config["work_schema"],
            host=db_config["host"],
            port=db_config["port"],
            role=db_config["role"],
        )
        yield storage
    except Exception:
        raise
    finally:
        if storage is not None:
            storage.close()
