import luigi
import os
from psycopg2 import sql

from cda_data_cleaning.common.read_config import read_config
from cda_data_cleaning.common.luigi_tasks import CDASubtask
from cda_data_cleaning.common.db_operations import create_connection

# TODO: Load these functions. Currently they are not used.


class CheckColumnExistence(CDASubtask):
    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    source_table = luigi.Parameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        config = read_config(str(self.config_file))
        schema = config["database-configuration"]["work_schema"]
        role = config["database-configuration"]["role"]

        conn = create_connection(config)

        query_create_function = sql.SQL(
            """
            SET ROLE {role};
            create or replace function {schema}.exist_columns(source_schema text, source_table text)
                 returns boolean
                    as
            $body$
            declare
                columns text[] := (select array_agg(column_name) from information_schema.columns
                    where table_schema = source_schema and table_name = source_table);
            begin
                --required columns
                if ('analysis_name_raw') = any(columns) and
                   ('parameter_name_raw') = any(columns) and
                   ('parameter_unit_raw') = any(columns) and
                   ('effective_time_raw') = any(columns) and
                   ('reference_values_raw') = any(columns) and
                   ('value_raw') = any(columns)
                then return TRUE;
                end if;
                return FALSE;
            end
            $body$
            language plpgsql;"""
        ).format(role=sql.Literal(role), schema=sql.Identifier(schema), table=sql.Identifier(self.source_table))

        cur = conn.cursor()
        cur.execute(query_create_function)
        conn.commit()

        query = sql.SQL("select %s.exist_columns(%%s, %%s);" % schema)
        cur.execute(query, [schema, self.source_table])
        results = cur.fetchall()
        columns_match = results[0][0]

        if columns_match is False:
            raise Exception("Columns in the source table do not contain all the required columns!")

        conn.commit()
        conn.close()
        self.mark_as_complete()


class CheckTableExistence(CDASubtask):
    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    source_table = luigi.Parameter()
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        config = read_config(str(self.config_file))
        schema = config["database-configuration"]["work_schema"]
        role = config["database-configuration"]["role"]

        conn = create_connection(config)

        query_create_function = sql.SQL(
            """
            set role {role};
            set search_path to {schema};

            create or replace function exist_table(source_schema text, source_table text)
                 returns boolean
                    as
            $body$
            declare

            begin
                if exists(
                           SELECT 1
                           FROM information_schema.tables
                           WHERE table_schema = source_schema
                             AND table_name = source_table
                           ) then return TRUE;
                end if;
                return FALSE;
                end;
            $body$
            language plpgsql;
            """
        ).format(schema=sql.Identifier(schema), role=sql.Identifier(role))

        cur = conn.cursor()
        cur.execute(query_create_function)
        conn.commit()

        query = sql.SQL("select %s.exist_table(%%s, %%s);" % schema)
        cur.execute(query, [schema, self.source_table])
        results = cur.fetchall()

        if results[0][0] is False:
            raise Exception("Source table ", self.source_table, " does not exist!")

        conn.commit()
        conn.close()

        self.mark_as_complete()
