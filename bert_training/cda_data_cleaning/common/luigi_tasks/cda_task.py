import os
import luigi
import psycopg2
import hashlib

from datetime import datetime
from configparser import ConfigParser
from typing import Union, List

from estnltk.storage import PostgresStorage
from psycopg2 import sql


class CDATask(luigi.Task):
    """
    A wrapper class around luigi.Task which hides standard bookkeeping code:
    - Each CDA tasks is defined as subclass of CDATask.
    - A CDA tasks use local files to indicate their completion.
    - CDA tasks are organised into workflows. Each workflow is in a separate folder.
    - Different workflows can define different tasks with the same class names as long as they are in different folders.
    - Workflows can use the same class to complete same task with different parameters. But then the tasks must have
      different target names (luigi_target_name) or otherwise one task is not scheduled.

    Each subclass of CDATask must define two instance variables:
    - luigi_target_name
    - luigi_targets_folder

    Each subclass of CDATask must use mark_as_complete in the run method to indicate that the task was successfully
    completed. This creates an empty local maker file that is used as local Luigi target.

    If you use the same task with different parameters different instances must have a different luigi_target_name.
    The easiest way to achieve this is to define a single class variable luigi_target_name and use initalisation
    to append the corresponding parameter value to self.luigi_target_name.


    Each subclass should be decorated with metainfo about its database usage and modification patterns.
    This can be done by defining four class variables:
    - source_tables
    - target_tables
    - altered_tables
    - deleted_tables
    """

    luigi_target_name = ""
    luigi_targets_folder = ""

    source_tables: List[str] = []
    target_tables: List[str] = []
    altered_tables: List[str] = []
    deleted_tables: List[str] = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Let give a unique target file for each class instance
        if len(self.luigi_target_name) == 0:
            self.luigi_target_name = "{task}_{hash}".format(task=self.get_task_family(), hash=self.hash())

    def hash(self):
        """
        Takes the parameters given for the CDATask and makes a unique hash out of them

        BUG: This function completely ignores differences in ObjectParameters and this can cause subtle errors
             where calling the same task with different ObjectParameter can be ignored completely!
             Bug is caused by self.to_str_params(). ObjectParameter bypasses its assumptions!
             Reason: ParameterVisibility.PRIVATE and thus it Task.to_str_params(...) always ignores the parameter
             Even if we set ParameterVisibility.HIDDEN we get wrong result as serialisation provides only class name

        SOLUTION: Either it is a WTF feature or we have to redefine CDATask.hash function to loop over private parameters
                  and use class hash instead of default parameter serialisation. Nothing hard but still things to do
        """

        params = "".join(["{0}{1}".format(k, v) for k, v in self.to_str_params().items()])
        h = hashlib.new("ripemd160")
        h.update(str.encode(params))
        hashed_params = h.hexdigest()
        return hashed_params

    def mark_as_complete(self):
        self.output().open("w").close()

    @staticmethod
    def log_current_time(message: str):
        """
        TODO: Find a good name for the function
        """
        print("------------------------------------------------------------------------------------------------------")
        print(message, datetime.now())
        print("------------------------------------------------------------------------------------------------------")

    @staticmethod
    def log_current_action(action: str):
        """
        TODO: Find a good name for the function
        """
        print(action.center(102, "-"))

    def log_dependencies(self):
        """
        Logs what tables are used, created, altered and deleted by a subtask.
        """
        if len(self.source_tables) > 0:
            print("\nSource tables:\n * ")
            print("\n* ".join(self.source_tables))
        if len(self.target_tables) > 0:
            print("\nTarget tables:\n * ")
            print("\n* ".join(self.target_tables))
        if len(self.altered_tables) > 0:
            print("\nAltered tables:\n * ")
            print("\n* ".join(self.altered_tables))
        if len(self.deleted_tables) > 0:
            print("\nDeleted tables:\n * ")
            print("\n* ".join(self.deleted_tables))
        print("\n")

    def log_schemas(self):
        """
        Logs what schemas are used by a subtask.
        """
        if hasattr(self, "source_schema"):
            print("\nSource schema: ")
            print("*", self.source_schema)
        if hasattr(self, "target_schema"):
            print("\nTarget schema:")
            print("*", self.target_schema)
        if hasattr(self, "schema"):
            print("\nSchema:")
            print("*", self.schema)
        if hasattr(self, "work_schema"):
            print("\nWork schema:")
            print("*", self.work_schema)
        print("\n")

    def output(self):
        sub_folder = os.path.join(*self.__module__.split(".")[1:])
        return luigi.LocalTarget(os.path.join(self.luigi_targets_folder, sub_folder, self.luigi_target_name))

    @staticmethod
    def read_config(conf_file: Union[str, luigi.Parameter], use_env: bool = True) -> ConfigParser:
        """
        Read configuration ini file and rewrite some of its entries by the environment variables.
        The file must have Microsoft Windows INI structure.
        The conf_file is expanded to absolute path and thus can be any Unix like path, e.g. ~/dir

        When both CDA_WORKFLOW_USERNAME and CDA_WORKFLOW_PASSWORD env variables exist and use_env flag is set the
        main DB configuration is changed accordingly. On command line the corresponding variables can be set

            export CDA_WORKFLOW_USERNAME=myNewFancyUsername
            export CDA_WORKFLOW_PASSWORD=myDatabasePassword

        and deleted

            unset CDA_WORKFLOW_USERNAME
            unset CDA_WORKFLOW_PASSWORD
        """

        config = ConfigParser()

        # By default Python does not work with Unix paths like ~/dir
        # Use magic lines to convert path into absolute path. This avoids WTF errors
        file_name = os.path.abspath(os.path.expanduser(os.path.expandvars(str(conf_file))))

        if not os.path.exists(file_name):
            raise ValueError("File {file} does not exist".format(file=str(conf_file)))

        if len(config.read(file_name)) != 1:
            raise ValueError("File {file} is not accessible or is not in valid INI format".format(file=conf_file))

        if not use_env:
            return config

        env_username = os.getenv("CDA_WORKFLOW_USERNAME")
        env_password = os.getenv("CDA_WORKFLOW_PASSWORD")

        if env_username is not None and env_password is not None:
            config["database-configuration"]["username"] = env_username
            config["database-configuration"]["password"] = env_password

        return config

    @staticmethod
    def create_postgres_connection(
        config: Union[str, ConfigParser, luigi.Parameter], db_section: str = "database-configuration"
    ):
        """
        Creates a psycopg2 connection to a postgres database specified by a configuration file.
        By default connection parameters are specified in the section [database-configuration].
        Returns a connection object or raises a value error with appropriate diagnostic message.

        Note that different Luigi tasks cannot share the same connection and you have to create separate
        connection for each task and close connections after all database operations are done in a task.
        """

        if isinstance(config, ConfigParser):
            file_name = ""
            config_parser = config
        else:
            # For clarity expand path into absolute path. This avoids WTF errors
            file_name = os.path.abspath(os.path.expanduser(os.path.expandvars(str(config))))
            config_parser = CDATask.read_config(file_name)

        if not config_parser.has_section(db_section):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section=db_section))
        for option in ["host", "port", "database_name", "username", "password"]:
            if not config_parser.has_option(db_section, option):
                prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
                raise ValueError(
                    "{prelude}Missing option {option} in the section [{section}]".format(
                        prelude=prelude, option=option, section=db_section
                    )
                )

        return psycopg2.connect(
            "postgresql://{username}:{password}@{host}:{port}/{database}".format(
                username=config_parser[db_section]["username"],
                password=config_parser[db_section]["password"],
                host=config_parser[db_section]["host"],
                port=config_parser[db_section]["port"],
                database=config_parser[db_section]["database_name"],
            )
        )

    @staticmethod
    def create_estnltk_storage(
        config: Union[str, ConfigParser, luigi.Parameter],
        db_section: str = "database-configuration",
        work_schema: str = None,
        role: str = None,
    ):
        """
        Creates EstNLTK PostgresStorage object with parameters specified by a configuration file.
        By default database connection parameters are specified in the section [database-configuration].
        Returns a storage object or raises a value error with appropriate diagnostic message.

        Two optional arguments are used to specify optional parameters for storage:
        - work_schema: a schema where EstNLTK collections are stored
        - role: a role that is used to access the storage and collections inside of it.

        Note that different Luigi tasks cannot share the database connections and thus you have to create
        separate storage object for each task and close the storage object after all operations with
        collection are done in a task. Otherwise, database resources are leaked!
        """

        if isinstance(config, ConfigParser):
            file_name = ""
            config_parser = config
        else:
            # For clarity expand path into absolute path. This avoids WTF errors
            file_name = os.path.abspath(os.path.expanduser(os.path.expandvars(str(config))))
            config_parser = CDATask.read_config(file_name)

        # Fetch database parameters from the configuration
        if not config_parser.has_section(db_section):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section=db_section))
        for option in ["host", "port", "database_name", "username", "password"]:
            if not config_parser.has_option(db_section, option):
                prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
                raise ValueError(
                    "{prelude}Missing option {option} in the section [{section}]".format(
                        prelude=prelude, option=option, section=db_section
                    )
                )

        # Fetch role and schema values if it is not set but are present in the configuration
        if role is None and config_parser.has_option(db_section, "role"):
            role = config_parser[db_section]["role"]
        if work_schema is None and config_parser.has_option(db_section, "work_schema"):
            work_schema = config_parser[db_section]["work_schema"]

        if role is None and work_schema is None:
            return PostgresStorage(
                dbname=config_parser[db_section]["database_name"],
                user=config_parser[db_section]["username"],
                password=config_parser[db_section]["password"],
                host=config_parser[db_section]["host"],
                port=config_parser[db_section]["port"],
            )
        elif role is None:
            return PostgresStorage(
                dbname=config_parser[db_section]["database_name"],
                user=config_parser[db_section]["username"],
                password=config_parser[db_section]["password"],
                schema=work_schema,
                host=config_parser[db_section]["host"],
                port=config_parser[db_section]["port"],
            )
        elif work_schema is None:
            return PostgresStorage(
                dbname=config_parser[db_section]["database_name"],
                user=config_parser[db_section]["username"],
                password=config_parser[db_section]["password"],
                host=config_parser[db_section]["host"],
                port=config_parser[db_section]["port"],
                role=role,
            )
        else:
            return PostgresStorage(
                dbname=config_parser[db_section]["database_name"],
                user=config_parser[db_section]["username"],
                password=config_parser[db_section]["password"],
                schema=work_schema,
                host=config_parser[db_section]["host"],
                port=config_parser[db_section]["port"],
                role=role,
            )

    @staticmethod
    def db_table_exists(conn, table: str, schema: str = "public") -> bool:
        """
        Returns if the database table exists in a given psycopg2 connection.
        The function does not work for long table names in the format schema.table!
        """
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                """
            select exists(
                select * from information_schema.tables where 
                table_name={table} and 
                table_schema={schema} and 
                table_catalog=current_database());
            """
            ).format(table=sql.Literal(table), schema=sql.Literal(schema))
        )

        result = cur.fetchone()[0]
        cur.close()
        return result

    @staticmethod
    def db_schema_exists(conn, schema: str) -> bool:
        """
        Returns if the database schema exists in a given psycopg2 connection.
        """
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                """
                SELECT exists(
                    SELECT schema_name 
                    FROM information_schema.schemata
                    WHERE schema_name = {schema});
                """
            ).format(schema=sql.Literal(schema))
        )

        result = cur.fetchone()[0]
        cur.close()
        return result

    @staticmethod
    def db_table_column_exists(conn, schema: str, table: str, column: str) -> bool:
        """
        Returns if the column exists in the given table.
        """
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                """
                SELECT EXISTS(SELECT *
                FROM information_schema.columns
                WHERE table_schema = {schema}
                    AND table_name = {table}
                    AND column_name = {column});
                """
            ).format(schema=sql.Literal(schema), table=sql.Literal(table), column=sql.Literal(column))
        )

        result = cur.fetchone()[0]
        cur.close()
        return result

    @staticmethod
    def estnltk_collection_exists(conn, collection_name: str, schema: str = "public") -> bool:
        """
        Return if the EstNLTK collection exists in a given psycopg2 connection.
        The function checks if the main database table with EstNLTK naming convention exists.
        No further validity checks are done.
        """
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                """
            select exists(
                select * from information_schema.tables where 
                table_name={table} and 
                table_schema={schema} and 
                table_catalog=current_database());
            """
            ).format(table=sql.Literal(collection_name), schema=sql.Literal(schema))
        )

        result = cur.fetchone()[0]
        cur.close()
        return result
