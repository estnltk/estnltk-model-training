import os
import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDATask
from cda_data_cleaning.common.db_operations import import_csv_file


class CreateAllLoincMappings(CDATask):
    """
    Creates all tables needed to assign LOINC codes to measurements and analysis.
    TODO: Make sure that different targets are created for different prefixes.
    Provably is but document why.
    """

    prefix = luigi.Parameter(default="")
    config_file = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        task_01 = LoincCodeMapping(
            self.prefix, self.config_file, self.schema, self.role, self.luigi_targets_folder, self.requirement
        )
        task_02 = LoincUnitMapping(
            self.prefix, self.config_file, self.schema, self.role, self.luigi_targets_folder, self.requirement
        )
        task_03 = LongLoincMapping(
            self.prefix, self.config_file, self.schema, self.role, self.luigi_targets_folder, self.requirement
        )
        return [task_01, task_02, task_03]

    def run(self):
        self.mark_as_complete()


class LoincCodeMapping(CDATask):
    """
    Creates all tables needed for simple assignment of LOINC codes to analysis results.
    """

    prefix = luigi.Parameter(default="")
    config_file = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        self.log_current_action("LOINC1: Create LOINC mappings")
        self.log_schemas()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        target_schema = str(self.schema)
        table_prefix = str(self.prefix) + "_" if len(str(self.prefix)) > 0 else ""
        data_folder = os.path.join(os.path.dirname(__file__), "data/")

        basename = "parameter_name_to_loinc_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{table};
            create table {schema}.{table}
            (
                parameter_name varchar,
                t_lyhend varchar,
                loinc_code varchar,
                substrate varchar, 
                evidence varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)
        change_empty_string_to_null(conn, target_schema, target_table, "substrate")

        basename = "parameter_name_unit_to_loinc_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{table};
            create table {schema}.{table}
            (
                parameter_name varchar,
                parameter_unit varchar,
                t_lyhend varchar,
                loinc_code varchar,
                substrate varchar,
                evidence varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)
        change_empty_string_to_null(conn, target_schema, target_table, "substrate")

        basename = "elabor_parameter_name_to_loinc_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{table};
            create table {schema}.{table}
            (
                parameter_name varchar, 
                t_lyhend varchar,
                t_nimetus varchar,
                kasutatav_nimetus varchar,
                loinc_code varchar,
                substrate varchar,
                evidence varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)
        change_empty_string_to_null(conn, target_schema, target_table, "substrate")

        basename = "elabor_parameter_name_unit_to_loinc_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{table};
            create table {schema}.{table}
            (
                parameter_name varchar, 
                t_yhik varchar, 
                t_lyhend varchar,
                t_nimetus varchar,
                kasutatav_nimetus varchar,
                loinc_code varchar,
                substrate varchar,
                evidence varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)
        change_empty_string_to_null(conn, target_schema, target_table, "substrate")

        # mapping panels
        basename = "elabor_analysis_name_to_panel_loinc"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{table};
            create table {schema}.{table}
            (
                analysis_name varchar,
                loinc_code varchar,
                t_lyhend varchar,
                t_nimetus varchar, 
                kasutatav_nimetus varchar,
                substrate varchar, 
                evidence varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)
        change_empty_string_to_null(conn, target_schema, target_table, "substrate")

        basename = "analysis_parameter_name_to_loinc_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
                set role {role};
                drop table if exists  {schema}.{table};
                create table {schema}.{table}
                (   analysis_name varchar,
                    parameter_name varchar,
                    t_lyhend varchar,
                    loinc_code varchar,
                    substrate varchar, 
                    evidence varchar
                );
                reset role;
                """
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)
        change_empty_string_to_null(conn, target_schema, target_table, "substrate")

        basename = "analysis_parameter_name_unit_to_loinc_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
                set role {role};
                drop table if exists  {schema}.{table};
                create table {schema}.{table}
                (   analysis_name varchar,
                    parameter_name varchar,
                    parameter_unit varchar,
                    t_lyhend varchar,
                    loinc_code varchar,
                    substrate varchar, 
                    evidence varchar
                );
                reset role;
                """
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)
        change_empty_string_to_null(conn, target_schema, target_table, "substrate")

        basename = "analysis_name_parameter_unit_to_loinc_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
                set role {role};
                drop table if exists  {schema}.{table};
                create table {schema}.{table}
                (   analysis_name varchar,
                    parameter_unit varchar,
                    t_lyhend varchar,
                    loinc_code varchar,
                    substrate varchar, 
                    evidence varchar
                );
                reset role;
                """
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)
        change_empty_string_to_null(conn, target_schema, target_table, "substrate")

        cur.close()
        conn.close()
        self.mark_as_complete()


class LoincUnitMapping(CDATask):
    """
    Creates tables for cleaning and mapping measurement units of analysis results
        - 'parameter_unit_to_cleaned_unit'
            has units which loinc_unit (t_yhik) are unknown
            could be manually improved
            columns: parameter_unit, parameter_unit_cleaned, loinc_unit (empty column), evidence (empty_column)
        - 'parameter_unit_to_loinc_unit_mapping'
            assigns to each parameter_unit a cleaned parameter_unit and loinc_unit (t_yhik)
            columns: parameter_unit, parameter_unit_cleaned, loinc_unit, evidence
    """

    prefix = luigi.Parameter(default="")
    config_file = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

        cur.close()
        conn.close()
        self.mark_as_complete()

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        target_schema = str(self.schema)
        table_prefix = str(self.prefix) + "_" if len(str(self.prefix)) > 0 else ""
        data_folder = os.path.join(os.path.dirname(__file__), "data/")

        basename = "parameter_unit_to_cleaned_unit"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{table};
            create table {schema}.{table}
            (
                parameter_unit varchar,
                parameter_unit_clean varchar,
                loinc_unit varchar,
                evidence varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)

        basename = "parameter_unit_to_loinc_unit_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{table};
            create table  {schema}.{table}
            (
                parameter_unit varchar,
                parameter_unit_clean varchar,
                loinc_unit varchar,
                evidence varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table)

        cur.close()
        conn.close()
        self.mark_as_complete()


class LongLoincMapping(CDATask):
    """
    Creates all tables needed for refined assignment of LOINC codes to analysis results.

    Not used right now.
    """

    prefix = luigi.Parameter(default="")
    config_file = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    role = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        return [self.requirement]

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        target_schema = str(self.schema)
        table_prefix = str(self.prefix) + "_" if len(str(self.prefix)) > 0 else ""
        data_folder = os.path.join(os.path.dirname(__file__), "data/")

        basename = "long_loinc_mapping"
        csv_file = data_folder + basename + ".csv"
        target_table1 = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{table};
            create table {schema}.{table}
            (
                analysis_name varchar,
                parameter_name varchar,
                parameter_code varchar,
                unit varchar,
                analyte varchar,
                property varchar,
                system varchar,
                time_aspect varchar,
                scale varchar
            );
            reset role;"""
            ).format(
                schema=sql.Identifier(target_schema), table=sql.Identifier(target_table1), role=sql.Literal(self.role)
            )
        )
        conn.commit()
        import_csv_file(conn, csv_file, target_schema, target_table1)

        basename = "long_loinc_mapping_unique"
        target_table2 = table_prefix + basename
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {schema}.{target_table};
            create table  {schema}.{target_table} as
                select analysis_name, parameter_name, unnest(array_agg(system)) as substrate
                from {schema}.{source_table}
                group by analysis_name, parameter_name
                having count(*) = 1;
            reset role;
                        
            update {schema}.{target_table}
            set substrate = case
                                when lower(analysis_name) ~ 'uriin' then 'urine'
                                when lower(analysis_name) ~ ('veri|vere|hemogramm|hemato') then 'bld'
                                when lower(analysis_name) ~ ('seerumis|plasma') then 'ser/plas'
                                else lower(substrate)
                end
            where substrate is null;
            """
            ).format(
                schema=sql.Identifier(target_schema),
                role=sql.Literal(self.role),
                source_table=sql.Identifier(target_table1),
                target_table=sql.Identifier(target_table2),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


def change_empty_string_to_null(conn, target_schema, target_table, target_column):
    """
    Converts all the empty strings in the given column to NULL.
    """
    cur = conn.cursor()
    cur.execute(
        sql.SQL(
            """
            UPDATE {target_schema}.{target_table} SET {target_column} = NULL WHERE {target_column} = '';
            """
        ).format(
            target_schema=sql.Identifier(target_schema),
            target_table=sql.Identifier(target_table),
            target_column=sql.Identifier(target_column),
        )
    )
    conn.commit()
