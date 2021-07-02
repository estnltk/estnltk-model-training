# This file here should replace configuration.py (as it is legacy in the sense of new configuration file)
# Yet, some stuff is missing from read_config.py and db_operations.py for our purposes, to support some older tasks
from cda_data_cleaning.common.db_operations import create_database_connection_string
from cda_data_cleaning.common.read_config import read_config


# Used to be POSTGRES_USERNAME and POSTGRES_PASSWORD;
# those are usually user-specific values -- we do not want to rely on those for app-specific production execution.
# Thus, we use app-specific env. variables:
#   USERNAME = os.getenv('CDA_WORKFLOW_USERNAME')
#   PASSWORD = os.getenv('CDA_WORKFLOW_PASSWORD')

# So far, it was assumed that the configuration file (conf.ini) is in certain fixed location (<checkout>/conf.ini);
# configuration.py read configurations like so:
#   import pathlib
#   config = configparser.ConfigParser()
#   config.read(os.path.join(pathlib.Path(__file__).parent.as_posix(), 'conf.ini'))
#
# New conf is passed via luigi variable like so:
#   PYTHONPATH='.' luigi ... --config-file=egcut_epi_microrun.app_extractor.ini --workers=3
# and passed through tasks; this file must be in <checkout>/configuration directory.
# Every task creates its own ConfigParser object:
#   config = read_config(str(self.config_file))
#   schema = config['database-configuration']['work_schema']
#   role = config['database-configuration']['role']

# How to handle passing around vs reading from fix position?
# What we do: We still use fixed file name for production (whole pipeline) processing:
#   - Execution passes in some-config-file.ini (to bash script)
#   - We make copy of it: cp configuration/some-config-file.ini configuration/db_conf.ini (in bash script)
#   - Luigi tasks taking in config file get in this fixed file name, 'db_conf.ini'
#   - read_config() is smart and checks for env.variables too
#       - when present, env.variable (CDA_WORKFLOW_USERNAME, CDA_WORKFLOW_PASSWORD) values
#         are used instead 'username' and 'password' values in configuration file
#       - NB! This allows to supply fully ready configurations that are under rev.control already,
#             except password (username can be in configuration but env.variable value is used)
config = read_config("db_conf.ini")

# Main DB section and its items
SECTION__MAIN_DB = "database-configuration"
SECTION_ITEM__MAIN_DB_SRC_SCHEMA = "original_schema"
SECTION_ITEM__MAIN_DB_TARGET_SCHEMA = "work_schema"
SECTION_ITEM__MAIN_DB_NAME = "database_name"
SECTION_ITEM__MAIN_DB_USERNAME = "username"
SECTION_ITEM__MAIN_DB_PASSWORD = "password"
SECTION_ITEM__MAIN_DB_TARGET_SCHEMA_ROLE = "role"

# Section item values in main DB section
main_db_conf = config[SECTION__MAIN_DB]
ORIGINAL_SCHEMA = main_db_conf[SECTION_ITEM__MAIN_DB_SRC_SCHEMA]
WORK_SCHEMA = main_db_conf[SECTION_ITEM__MAIN_DB_TARGET_SCHEMA]
DATABASE_NAME = main_db_conf[SECTION_ITEM__MAIN_DB_NAME]

# Used from places like:
#   - drug_data_cleaning/clean_values.py
#   - diagnosis_data_cleaning/create_table.py
# Query more examples via usage like 'from configuration import.* database_connection_string'
database_connection_string = create_database_connection_string(main_db_conf)


# Variable 'role_name' is used in places like:
#   - common/read_and_update.py
#   - cda_data_cleaning/shared_workflows/loinc_cleaning/development/loinc_analyte_property_system_assignment/main.py
#   - legacy/utils.py
#   - epicrisis_date_cleaning/step_01_copy_tables.py
# Query more examples via usages like 'from configuration import.*role'
role_name = main_db_conf.get(SECTION_ITEM__MAIN_DB_TARGET_SCHEMA_ROLE, None)

# NOTE: db_operations.py provides:
#  - create_database_connection_string(db_conf)
#  - create_connection(config) -- returns whatever pg2.connect() returns
# Example: diagnosis_data_cleaning/create_table.py uses old config like so:
#   from configuration import database_connection_string, ORIGINAL_SCHEMA, WORK_SCHEMA
#   ...
#   engine = sq.create_engine(database_connection_string, encoding='utf-8', convert_unicode=True)
#
#   connection = engine.connect()
#   meta_original = sq.MetaData(bind=connection, schema=ORIGINAL_SCHEMA)
#   meta_original.reflect(connection, views=True)
#   meta_work = sq.MetaData(bind=connection, schema=WORK_SCHEMA)
#   meta_work.reflect(connection)


#
# Classification DB section
#
# Used likes so:
#   from configuration import classifications_database_connection_string, CLASSIFICATIONS_SCHEMA
#   (diagnosis_data_cleaning/icd10.py)
classifications_db_conf = config["classifications-database-configuration"]
CLASSIFICATIONS_SCHEMA = classifications_db_conf["schema"]

# Note that there are no separate declarations for 'username' and 'password' in classifications DB;
# the user who can access main DB shall be able to access classifications DB as well
classifications_database_connection_string = "postgresql://{username}:{password}@{host}:{port}/{database}".format(
    username=main_db_conf[SECTION_ITEM__MAIN_DB_USERNAME],
    password=main_db_conf[SECTION_ITEM__MAIN_DB_PASSWORD],
    host=classifications_db_conf["host"],
    port=classifications_db_conf["port"],
    database=classifications_db_conf["database_name"],
)


#
# Luigi related configuration
#
# Used like so:
#   from configuration import luigi_targets_folder
#   (workflow/cleaning_tasks.py)
luigi_conf = config["luigi"]
luigi_targets_folder = luigi_conf["folder"]
