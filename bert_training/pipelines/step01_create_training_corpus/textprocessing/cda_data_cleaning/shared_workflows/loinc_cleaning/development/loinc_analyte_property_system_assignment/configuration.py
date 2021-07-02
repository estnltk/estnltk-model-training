import configparser
import os
import pathlib

PASSWORD = os.getenv("POSTGRES_PASSWORD")
USERNAME = os.getenv("POSTGRES_USERNAME")

config = configparser.ConfigParser()

config.read(os.path.join(pathlib.Path(__file__).parent.as_posix(), "conf.ini"))

db_conf = config["database-configuration"]
classifications_db_conf = config["classifications-database-configuration"]

ORIGINAL_SCHEMA = db_conf["original_schema"]
WORK_SCHEMA = db_conf["work_schema"]
DATABASE_NAME = db_conf["database_name"]


database_connection_string = "postgresql://{username}:{password}@{host}:{port}/{database}".format(
    username=USERNAME, password=PASSWORD, host=db_conf["host"], port=db_conf["port"], database=DATABASE_NAME
)

CLASSIFICATIONS_SCHEMA = classifications_db_conf["schema"]

classifications_database_connection_string = "postgresql://{username}:{password}@{host}:{port}/{database}".format(
    username=USERNAME,
    password=PASSWORD,
    host=classifications_db_conf["host"],
    port=classifications_db_conf["port"],
    database=classifications_db_conf["database_name"],
)

# let's not keep these around for too long
del USERNAME
del PASSWORD

role_name = db_conf.get("role", None)

luigi_conf = config["luigi"]
luigi_targets_folder = luigi_conf["folder"]
