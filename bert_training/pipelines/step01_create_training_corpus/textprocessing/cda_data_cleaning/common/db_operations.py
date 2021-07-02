import warnings
import psycopg2 as pg2
import psycopg2.extras
import pandas as pd

from psycopg2 import sql
from psycopg2.sql import Composed, SQL, Identifier


def insert_to_database(dataframe, schema_dot_table_name, conn):
    """
    Inserts pandas dataframe to database.

    Does not send querys which size is greater than 5000000
    :param dataframe:
    :param schema_dot_table_name:
    :param conn:
    """

    cur = conn.cursor()

    df_columns = list(dataframe)
    columns = ",".join(df_columns)

    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

    insert_stmt = "INSERT INTO {} ({}) {}".format(schema_dot_table_name, columns, values)

    if get_query_length(sql.SQL(insert_stmt)) < 5000000:
        # esceping is automatic, works with  '', /, % etc
        # https://stackoverflow.com/questions/3823735/psycopg2-equivalent-of-mysqldb-escape-string
        # values = [['1','13389160 ','0','Biokeemia '' labor','S-Gluc,mmol/l', '', ' 3,9- % 6,0','16.04.2014','5,5', '']]

        pg2.extras.execute_batch(cur, insert_stmt, dataframe.values)
        conn.commit()
    else:
        warnings.warn("Too big table, did not send it to database!")


def import_csv_file(conn, csv_file, target_schema, table_name):
    # TODO: Use postgres csv import instead using pandas here!
    # The following might not work as it seems to use COPY function that is for suoeruser
    # https://kb.objectrocket.com/postgresql/python-import-csv-into-postgres-908
    # https://groups.google.com/forum/#!topic/luigi-user/aqhhyUaber8
    mapping = pd.read_csv(csv_file, keep_default_na=False)
    insert_to_database(mapping, target_schema + "." + table_name, conn)


def get_query_length(q):
    """:returns
    approximate number of characters in the psycopg2 SQL query
    """
    result = 0
    if isinstance(q, Composed):
        for r in q:
            result += get_query_length(r)
    elif isinstance(q, (SQL, Identifier)):
        result += len(q.string)
    else:
        result += len(str(q.wrapped))
    return result


def create_database_connection_string(db_conf):
    """Create PostgreSQL database connection string.

    :param db_conf: Mapping[str, str]
        dict with keys `username`, `password`, `host`, `port`, `database_name`
    :return: str
        PostgreSQL database connection string
    """
    return "postgresql://{username}:{password}@{host}:{port}/{database}".format(
        username=db_conf["username"],
        password=db_conf["password"],
        host=db_conf["host"],
        port=db_conf["port"],
        database=db_conf["database_name"],
    )


# Important note concerning Luigi and DB connections:
# https://github.com/spotify/luigi/issues/2782#issuecomment-544539769 interpreted:
#   Database connections are not typically shared over different processes.
#   You'll need to be creating the database connection in the luigi tasks/targets that are you using
#   and not relying on any ORM connection objects passed into your worker.
#
#   Try setting force_multiprocessing=True in your worker config and I'll guess you'll see the same
#   errors when running with workers=1.
#
#   When using multiprocessing in Python, you should close all connection every time a process is spawned.
#   Therefore in Luigi inside every Task.
#     from ... import connection
#     connection.close()
def create_connection(config):
    """
    :param config: type ConfigParser from read_config.py
    :return:
    """
    return pg2.connect(create_database_connection_string(config["database-configuration"]))
