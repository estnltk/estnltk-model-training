import os
import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import import_csv_file


class ImportMappingTable(CDASubtask):
    """
    Importing the table from data folder to database which contains standard mapping from Ravimiamet.
    TODO: unify naming so that the tasks have same name as loinc table imports
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")  # This should be classifications probably
    target_table = luigi.Parameter(default="drug_packages")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    target_tables = ["drug_packages"]
    luigi_target_name = "map_missing_data_in_parsed_table"

    def run(self):
        self.log_current_action("Step 2 import drug package table")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};
            
            drop table if exists {schema}.{mapping};
            create table {schema}.{mapping}
            (
                package_type                       text,
                drug_type                          text,
                package_code                       integer,
                package_name                       text,
                atc_code                           text,
                active_substance                   text,
                active_substance_content           text,
                active_substance_class             text,
                dosage_form                        text,
                prescription                       text,
                prescription_type                  text,
                narc_type                          text,
                marketing_authorization_expiration text,
                authorization_holder               text,
                manufacturer                       text,
                safety_measure                     text,
                safety_pamphlet                    text,
                risk                               text,
                risk_description                   text,
                routes_of_administration           text
            );                
            reset role;
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                mapping=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        data_folder = os.path.join(os.path.dirname(__file__), "../step01_clean_entry/data/")
        # TODO: This is a bad practice. The file name is too specific
        # when somebody wants to update, she also has to update code which is somewhat surpising
        # On the same time versioning is good. Some compromise must be made.
        csv_file = data_folder + "drug_packages_202005131300.csv"
        import_csv_file(conn, csv_file, str(self.target_schema), str(self.target_table))

        cur.close()
        conn.close()
        self.mark_as_complete()


class PerformMapping(CDASubtask):
    """
    Updating '*_drug_parsed_cleaned', filling in the missing values.
    1. Fill in missing ingredients based on drug_name
    2. Fill in missing ingredients based on drug_code
    3. Fill in missing drug_codes based on active_ingredient and package_type
    4. Fill in missing drug_codes based on only active_ingredient

    TODO: Rename it updates/imputes table. Name should reflect this
    TODO: Update documentation
    Mappings are created to mappings schema
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    table = luigi.Parameter(default="drug_parsed_cleaned")
    mapping_schema = luigi.Parameter(default="work")
    mapping_table = luigi.Parameter(default="drug_packages")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    source_tables = ["source_parsed"]
    target_tables = ["mapping", "dn_to_ing", "dc_to_ing"]
    luigi_target_name = "map_missing_data_in_parsed_table"

    def run(self):
        self.log_current_action("Step 2 map missing atc codes and ingredients")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        main_table = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(str(self.table))
        )
        # Define necessary sub-mappings from the basic map
        main_map = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(str(self.mapping_table))
        )
        drug_name_to_ingredient_map = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(str(self.mapping_table) + "_dn_to_ing")
        )
        drug_code_to_ingredient_map = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(str(self.mapping_table) + "_dc_to_ing")
        )
        ingredient_package_type_to_drug_code_map = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(str(self.mapping_table) + "_ing_pt_to_dc")
        )
        ingredient_to_drug_code_map = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(str(self.mapping_table) + "_ing_to_dc")
        )

        # TODO: Do you really need these sub-mappings as tables if you always create them from scratch
        #       If you want later on add additional manual data into it then indeed but then you should
        #       separate creation of mappings into a separate step

        cur.execute(
            sql.SQL(
                """
            set role {role};
            
            -- based on drug_name/package_name (DN) map corresponding active_ingredient/active_substance (ING)
            -- note: DN is always more detailed
            drop table if exists {dn_to_ing};
            create table {dn_to_ing} as
            select distinct package_name, active_substance
            from {mapping};
            
            -- based on drug_code/atc_code (DC) map corresponding active_ingredient/active_substance (ING)
            -- Note: each DC has only one corresponding ING
            drop table if exists {dc_to_ing};
            create table {dc_to_ing} as
            select distinct atc_code, active_substance 
            from {mapping}
            where atc_code is not null and active_substance is not null;
            
            -- based on active_ingredient/active_substance (ING) and dosage_form/package_type(PT) 
            -- map corresponding drug_code/atc_code (DC)
            drop table if exists {ing_pt_to_dc};
            create table {ing_pt_to_dc} as
            select distinct atc_code, active_substance, dosage_form
            from {mapping};
                
            -- based on active_ingredient/active_substance (ING) map corresponding drug_code/atc_code (DC)
            -- note that one ING can have many corresponding DC
            -- therefore, only map ING that have ONLY ONE DC
            drop table if exists {ing_to_dc};
            create table {ing_to_dc} as
            with unique_substances as
            (
                select active_substance, array_agg(distinct atc_code) atc_codes
                from {mapping}
                group by active_substance
                having count(distinct atc_code) = 1
            )
            select active_substance, unnest(atc_codes) as atc_code
            from unique_substances;
            """
            ).format(
                role=sql.Identifier(self.role),
                mapping=main_map,
                dn_to_ing=drug_name_to_ingredient_map,
                dc_to_ing=drug_code_to_ingredient_map,
                ing_pt_to_dc=ingredient_package_type_to_drug_code_map,
                ing_to_dc=ingredient_to_drug_code_map,
            )
        )
        conn.commit()

        # Impute missing data from context if it is possible
        # TODO: Document why are updates in this particular order. Does it really matter?
        cur.execute(
            sql.SQL(
                """
            set role {role};
            
            -- only update rows where ING is missing
            update {main_table}
            set active_ingredient = mapping.active_substance
            from {dn_to_ing} as mapping
            where active_ingredient is null and drug_name = mapping.package_name;
            
            -- only update rows where ING is missing
            update {main_table}
            set active_ingredient = mapping.active_substance
            from {dc_to_ing} as mapping
            where active_ingredient is null and drug_code = mapping.atc_code; 
            
            -- only update rows where DC is missing
            update {main_table}
            set drug_code = mapping.atc_code
            from {ing_pt_to_dc} as mapping
            where 
                drug_code is null and
                active_ingredient = mapping.active_substance  and
                drug_form_administration_unit_code_display_name = mapping.dosage_form;
            
            -- only update rows where DC is missing
            update {main_table}
            set drug_code = mapping.atc_code
            from {ing_to_dc} as mapping
            where drug_code is null and active_ingredient = mapping.active_substance;
            """
            ).format(
                role=sql.Identifier(self.role),
                main_table=main_table,
                dn_to_ing=drug_name_to_ingredient_map,
                dc_to_ing=drug_code_to_ingredient_map,
                ing_pt_to_dc=ingredient_package_type_to_drug_code_map,
                ing_to_dc=ingredient_to_drug_code_map,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class LogMissingDataInParsed(CDASubtask):
    """
    Parsed drug table contains a lot of missing drug_codes (atc_codes) and ingredients.
    Mapping of missing values is based on standard table found from Ravimiamet's website.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="drug_parsed_cleaned")
    output_file = luigi.Parameter(default="")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    luigi_target_name = "map_missing_data_in_parsed_table"
    source_tables = ["drug_parsed_cleaned"]

    def run(self):
        self.log_current_action("Step 2 Log some results!?")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # missing drug_codes
        cur.execute(
            sql.SQL(
                """
            set search_path to {schema};
            SELECT COUNT(*) FROM {source} where drug_code is NULL;
            """
            ).format(schema=sql.Identifier(self.schema), source=sql.Identifier(self.source_table))
        )
        count_missing_drug_code = str(cur.fetchone()[0])

        # missing ingredients
        cur.execute(
            sql.SQL(
                """
            set search_path to {schema};
            SELECT COUNT(*) FROM {source} where active_ingredient is NULL;
            """
            ).format(schema=sql.Identifier(self.schema), source=sql.Identifier(self.source_table))
        )
        count_missing_active_ingredients = str(cur.fetchone()[0])

        # write information about output tables to log file
        with open(str(self.output_file), "a") as log_file:
            log_file.write("  After mapping missing data in parsed: \n")
            log_file.write(
                "     Step02: Cleaned parsed table has "
                + count_missing_drug_code
                + " rows with missing drug codes. \n"
            )
            log_file.write(
                "     Step02: Cleaned parsed table has "
                + count_missing_active_ingredients
                + " rows with missing active ingredients. \n"
            )

        cur.close()
        conn.close()
        self.mark_as_complete()
