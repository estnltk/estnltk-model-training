import os
import pathlib

import luigi
import sqlalchemy as sq
from configuration import WORK_SCHEMA

from configuration import database_connection_string
from configuration import role_name
from configuration import luigi_targets_folder
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import write_empty_file

loinc_mapping_table_name = "long_loinc_mapping"  # Target table where we will put CSV content


def create_table(connection, table, role_name):
    if role_name is not None:
        connection.execute("SET ROLE {}".format(role_name))
    table.create()
    if role_name is not None:
        connection.execute("RESET ROLE")


def get_static_loinc_mapping_table_name(table_prefix):
    return "{prefix}_{table_name}".format(prefix=table_prefix, table_name=loinc_mapping_table_name)


def get_schema_qualified_static_loinc_mapping_table_name(table_prefix):
    return WORK_SCHEMA + "." + get_static_loinc_mapping_table_name(table_prefix)


# Part of command line: ... --module analysis_data_cleaning.LOINC_cleaning.main LongLoincMappingTable
# Creates table work.{PREFIX}_long_loinc_mapping when it does not exist already
class LongLoincMappingTable(luigi.Task):
    prefix = luigi.Parameter()

    loinc_mapping_csv_file = "long_loinc_mapping.csv"  # Static mapping rules, mined beforehand
    loinc_mapping_csv_file_full_path = os.path.join(pathlib.Path(__file__).parent.as_posix(), loinc_mapping_csv_file)

    # Essentially, our table from CSV file looks like this (except that CSV file does not give 'id' field):
    #   id serial not null
    #     constraint {PREFIX}_long_loinc_mapping_pkey
    #       primary key,
    #   -- column name and type with descirption and example
    #   analysis_name TEXT,  -- name of the analysis                  | Hemogramm 5-osalise leukogrammiga
    #   parameter_name TEXT, -- name of the measured parameter        | BASO#
    #   parameter_code TEXT, -- code of the measured parameter        | B-CBC+Diff
    #   unit TEXT,           -- units that the parameter was measured | 10\9/L
    #   analyte TEXT,        -- mapped loinc analyte                  | Baso
    #   property TEXT,       -- mapped loinc property                 | Num
    #   system TEXT,         -- mapped loinc system                   | Bld
    #   time_aspect TEXT,    -- time when observation was made        | Pt
    #   scale TEXT           -- the scale of measure                  | Qn
    #
    # Basically, the mapping table maps analysis name, parameter name, parameter code and unit to
    # LOINC, i.e. to analyte, property, system, time_aspect and scale.
    columns = [
        "analysis_name",
        "parameter_name",
        "parameter_code",
        "unit",
        "analyte",
        "property",
        "system",
        "time_aspect",
        "scale",
    ]

    def run(self):
        prefixed_loinc_mapping_table_name = get_static_loinc_mapping_table_name(self.prefix)
        schema_qualified_mapping_table_name = get_schema_qualified_static_loinc_mapping_table_name(self.prefix)

        engine = sq.create_engine(database_connection_string, encoding="utf-8", convert_unicode=True)

        connection = engine.connect()
        meta_work = sq.MetaData(bind=connection, schema=WORK_SCHEMA)
        meta_work.reflect(connection)

        with connection.begin():
            loinc_mapping_table = meta_work.tables.get(schema_qualified_mapping_table_name, None)

            # Create LOINC mapping table only when it does not exist
            if loinc_mapping_table is None:
                loinc_mapping_table = sq.Table(prefixed_loinc_mapping_table_name, meta_work)

                # Similar to create_new_table(connection, new_table) elsewhere
                loinc_mapping_table.append_column(sq.Column("id", sq.BigInteger, primary_key=True, autoincrement=True))

                for column in self.columns:
                    loinc_mapping_table.append_column(sq.Column(column, sq.Text))

                create_table(connection, loinc_mapping_table, role_name)
                print("We have new empty table: " + schema_qualified_mapping_table_name)
        connection.close()

        # Insert content based on static LOINC mapping CSV file as well, this will be used in JOIN further on
        # PostgreSQL COPY reference: https://www.postgresql.org/docs/current/sql-copy.html
        # Example of CSV:
        #   analysis_name,parameter_name,parameter_code,unit,analyte,property,system,time_aspect,scale
        #   Hemogramm viieosalise leukogrammiga,MONO%,B-CBC-5Diff,%,Mono,NCnc,Bld,Pt,Qn
        #   Välisanalüüsid,"HSV1, HSV2 DN(Lihtherpesviiruse 1. ja 2. tüübi DNA)",10120,,,,Bld,Pt,Qn
        with open(self.loinc_mapping_csv_file_full_path) as f:
            conn = engine.raw_connection()
            conn.cursor()
            cursor = conn.cursor()
            cmd = """
            COPY {tbl}({column_list}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE, QUOTE '"')
            """.format(
                tbl=schema_qualified_mapping_table_name, column_list=", ".join(self.columns)
            )
            cursor.copy_expert(cmd, f)
            conn.commit()

        write_empty_file(os.path.join(luigi_targets_folder, self.prefix), self.output())

    def output(self):
        return luigi.LocalTarget(os.path.join(luigi_targets_folder, self.prefix, "LongLoincMappingTable_done"))


# Creates filled table work.{PREFIX}_analysis_loinced with proper roles; any existing tables will be dropped
class AnalysisLoincedTable(luigi.Task):
    prefix = luigi.Parameter()

    analysis_loinced_table_name = "analysis_loinced"  # Target table for LOINC-ed analyses, final results

    def get_prefixed_analysis_loinced_table_name(self):
        return "{prefix}_{table_name}".format(prefix=self.prefix, table_name=self.analysis_loinced_table_name)

    def get_schema_qualified_analysis_loinced_table_name(self):
        return WORK_SCHEMA + "." + self.get_prefixed_analysis_loinced_table_name()

    def create_and_fill_analysis_loinced_table(self):
        # Table, used in JOIN to get main content of analysis data
        schema_qualified_analysis_matched_table_name = "{schema}.{prefix}_analysis_matched".format(
            schema=WORK_SCHEMA, prefix=self.prefix
        )

        # Fixed table giving mapping from analysis info to LOINC properties
        schema_qualified_static_loinc_mapping_table_name = get_schema_qualified_static_loinc_mapping_table_name(
            self.prefix
        )

        # Our result-table where analysis info and LOINC properties are together
        prefixed_analysis_loinced_table_name = self.get_prefixed_analysis_loinced_table_name()
        schema_qualified_analysis_loinced_table_name = self.get_schema_qualified_analysis_loinced_table_name()

        # NB! For the JOIN condition there are several records in static LOINC mapping table, e.g.
        #       analysis_name='Biokeemilised uuringud' and
        #       parameter_name='S-Gluc(Glükoos seerumis)' and
        #       parameter_code='715'
        # gives two entries. Thus, analysis_loinced will be bigger that analysis_matched.
        sql = """
        DROP TABLE IF EXISTS {main_target_table};
        CREATE TABLE {main_target_table} AS
          SELECT
               -- In the end, we want also have our common 'id' IDENTITY column, typed as BIGINT, no NULLs, PRIMARY KEY.
               -- Initially, we create only sequentially generated 'id' values; after table creation we turn this
               -- 'id' column into proper IDENTITY column, with updated underlying sequence
               row_number() OVER (ORDER BY matched.epi_id, matched.effective_time, matched.analysis_name) id,
               matched.epi_id,
               long.analyte,
               long.property,
               long.system,
               long.time_aspect,
               long.scale,
               long.unit,
               matched.analysis_name,
               matched.parameter_name,
               matched.parameter_code_raw_entry AS parameter_code,
               matched.code_system_name AS code_system_type,
               matched.effective_time,
               matched.value,
               matched.reference_values,
               -- NB! In 'original' schema we use 'hcp' as 'health care provider', not 'tto'
               --     See also OHDSI / OMOP CDM concepts: 
               --       https://github.com/OHDSI/CommonDataModel/wiki/Standardized-Health-System-Data-Tables
               (SUBSTRING(matched.code_system, 
                          '[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*')) AS tto_oid
          FROM {main_src_table} as matched
          LEFT JOIN {static_mapping_table} as long
            ON long.analysis_name = matched.analysis_name AND
               long.parameter_name = matched.parameter_name AND
               long.parameter_code = matched.parameter_code_raw_entry
        ;

        -- Make 'id' column to proper IDENTITY column
        ALTER TABLE {main_target_table} ALTER COLUMN id SET NOT NULL;
        ALTER TABLE {main_target_table} ADD PRIMARY KEY (id);
        ALTER TABLE {main_target_table} ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;        
        -- Follwoing wokr to update corresponding sequence, but only with literal/constant values:
        --   ALTER TABLE {main_target_table} ALTER COLUMN id RESTART WITH 1000;
        -- Thus, we query for a sequence name and update it
        SELECT setval(
            pg_get_serial_sequence('{main_target_table}', 'id'),
            (SELECT max(id) FROM {main_target_table}),
            TRUE
        );
        
        -- Create some indexes as well
        CREATE INDEX {main_target_table_without_schema}_epi_id_idx 
            ON {main_target_table}(epi_id);
        
        CREATE INDEX {main_target_table_without_schema}_loinc_idx 
            ON {main_target_table}(analyte, property, system, time_aspect, scale, unit);
        
        CREATE INDEX {main_target_table_without_schema}_effective_time_idx 
            ON {main_target_table}(effective_time);        
        """.format(
            main_target_table_without_schema=prefixed_analysis_loinced_table_name,
            main_src_table=schema_qualified_analysis_matched_table_name,
            static_mapping_table=schema_qualified_static_loinc_mapping_table_name,
            main_target_table=schema_qualified_analysis_loinced_table_name,
        )

        # Execute our SQL now to create and fill analysis_loinced table
        engine = sq.create_engine(database_connection_string, encoding="utf-8", convert_unicode=True)

        connection = engine.connect()
        meta_work = sq.MetaData(bind=connection, schema=WORK_SCHEMA)
        meta_work.reflect(connection)

        with connection.begin():
            from common.read_and_update import RoleManager

            with RoleManager(connection, role_name):
                connection.execute(sq.sql.text(sql))
        connection.close()

        print("We have now created and filled table: " + schema_qualified_analysis_loinced_table_name)

    def run(self):
        # Create new table and fill it with analyses that have LOINC information attached
        self.create_and_fill_analysis_loinced_table()

        write_empty_file(os.path.join(luigi_targets_folder, self.prefix), self.output())

    def output(self):
        return luigi.LocalTarget(os.path.join(luigi_targets_folder, self.prefix, "AnalysisLoincedTable_done"))


# Create '{prefix}_analysis_loinced' table based on join of '{prefix}_analysis_matched'
# and '{prefix}_long_loinc_mapping' (raw SQL is in analysis_loinced.sql);
# '*_long_loinc_mapping' table comes from
#     analysis_data_cleaning/LOINC_cleaning/long_loinc_mapping.csv
# file (which in turn is mined from captured rules).
class LoincMapping(luigi.Task):
    prefix = luigi.Parameter()

    def requires(self):
        return LongLoincMappingTable(self.prefix)

    def run(self):
        yield AnalysisLoincedTable(self.prefix)

        write_empty_file(os.path.join(luigi_targets_folder, self.prefix), self.output())

    def output(self):
        return luigi.LocalTarget(os.path.join(luigi_targets_folder, self.prefix, "LoincMapping_done"))
