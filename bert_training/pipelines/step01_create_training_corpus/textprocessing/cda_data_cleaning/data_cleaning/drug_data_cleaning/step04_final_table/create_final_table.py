import os
import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask
from cda_data_cleaning.common.db_operations import import_csv_file


class CreateFinalTable(CDASubtask):
    """
    1. Create a table 'atc_to_omop' for mapping RXNORM concept_id (useful for future OMOP mapping)
    2. Creates final table called '*_drug_cleaned' from matched table called '*_drug_matched_wo_duplicates'
       with mapped RXNORM codes

    Final table has columns:
        epi_id, id_entry, id_parsed, id_original_drug, epi_type, effective_time, drug_code,
        active_ingredient, drug_name_parsed, dose_quantity_value_parsed, dose_quantity_unit_parsed,
        dose_quantity_value_entry, dose_quantity_unit_entry, rate_quantity_value, rate_quantity_unit,
        drug_form_administration_unit_code_display_name, drug_id_entry, prescription_type_entry,
        drug_form_administration_unit_code_entry, recipe_code_parsed, package_size_parsed,
        text_type_parsed, match_description, rxnorm_concept_id
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="drug_matched_wo_duplicates")
    target_table = luigi.Parameter(default="drug_cleaned")
    mapping_schema = luigi.Parameter(default="work")
    mapping_table = luigi.Parameter(default="atc_to_omop")
    output_file = luigi.Parameter(default="")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    luigi_target_name = "create_final_drug_table"
    source_tables = ["atc_to_omop", "drug_matched_wo_duplicates"]
    target_tables = ["drug_cleaned"]

    def run(self):
        self.log_current_action("Step04 create final table")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        atc_to_omop_map = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(str(self.mapping_table))
        )

        cur.execute(
            sql.SQL(
                """
            set search_path to {schema};
            set role {role};
            
            drop table if exists {target};
            create table {target} as
            with final_drug_cleaned as
            (
                select
                    -- matching step makes sure that there are no contradicting epi_id-s, so doesn't matter which one we choose
                    COALESCE(epi_id_parsed, epi_id_entry)  as epi_id,
                    id_entry,                                    -- id referring to original.drug_entry table
                    id_parsed,                                   -- id referring to drug_parsed table
                    id_original_drug_parsed as id_original_drug, --id referring back to original.drug table text column (parsed table is based on original.drug)
                    COALESCE(epi_type_parsed, epi_type_entry) as epi_type,
                    -- matching step makes sure that there are no contradicting effective_times (note: parsed table contains way more times)
                    COALESCE(effective_time_parsed, effective_time_entry)  as effective_time,
                    -- matching step makes sure that there are no contradicting drug_codes
                    COALESCE(drug_code_parsed, drug_code_entry) as drug_code,
                    -- matching step makes sure that there are no DIRECTLY contradicting ingredients
                    -- parsed table ingredient is a subset of entry table ingredient (latter is more detailed)
                    -- therefore always prefer more detailed column
                    COALESCE(active_ingredient_entry, active_ingredient_parsed) as active_ingredient,
                    COALESCE(active_ingredient_est_entry, active_ingredient_est_parsed) as active_ingredient_est,
                    COALESCE(active_ingredient_latin_entry, active_ingredient_latin_parsed) as active_ingredient_latin,
                    COALESCE(active_ingredient_eng_entry, active_ingredient_eng_parsed) as active_ingredient_eng,
                    drug_name_parsed,
                    -- it is not clear whether parsed or entry dose is "better"
                    -- for example in parsed: DOV = 2, DOU = tabletti and in the corresponding entry: DOV = 10, DOU = mg
                    -- for omop mapping dosages given with "tablett" (and not "mg") are more useful for calculating the drug exposure time
                    -- so we try to keep the values with "tablett"
                    -- if neither of the columns has "tablett" then choose the one where there are less NULLs in DOV and DOU
                    -- e.g.
            
                    -- parsed             entry
                    -- DOV DOU            DOV DOU
                    -- 2   mg             1   tilk
                    -- chooses 2 mg
            
                    -- parsed             entry
                    -- DOV DOU            DOV DOU
                    -- 2   NULL           1   tilk
                    -- chooses 1 tilk
            
                    -- parsed             entry
                    -- DOV DOU            DOV DOU
                    -- 2   NULL           1   NULL
                    -- chooses 1 NULL
            
                    -- parsed             entry
                    -- DOV DOU            DOV DOU
                    -- 2   NULL           NULL   NULL
                    -- chooses NULL NULL
                    case when dose_quantity_unit_parsed ~ '(ta|tbl|tab|tablett)' then dose_quantity_value_parsed
                         when dose_quantity_unit_entry ~ '(ta|tbl|tab|tablett)' then dose_quantity_value_entry
                         when dose_quantity_value_parsed is not null and dose_quantity_unit_parsed is not null then dose_quantity_value_parsed
                         else dose_quantity_value_entry
                        end as dose_quantity_value,
                    case when dose_quantity_unit_parsed ~ '(ta|tbl|tab|tablett)' then dose_quantity_unit_parsed
                         when dose_quantity_unit_entry ~ '(ta|tbl|tab|tablett)' then dose_quantity_unit_entry
                         when dose_quantity_value_parsed is not null and dose_quantity_unit_parsed is not null then dose_quantity_unit_parsed
                         else dose_quantity_unit_entry
                        end as dose_quantity_unit,
                    -- extra information extracted from origianl.drug_entry column drug_code_display_name
                    dose_quantity_extra_entry,
                    -- parsed table rate_quantity_value is a subset of entry table rate_quantity_value (latter is more detailed)
                    COALESCE(rate_quantity_value_entry, rate_quantity_value_parsed) as rate_quantity_value,
                    COALESCE(rate_quantity_unit_entry, rate_quantity_unit_parsed) as rate_quantity_unit,
                    -- entry contains more detailed information, so always prefer more detailed column
                    -- for example: parsed: 'ninasprei', entry: 'ninasprei, suspensioon'
                    COALESCE(dosage_form_entry, dosage_form_parsed) as dosage_form,
                    drug_id_entry,
                    prescription_type_entry,
                    drug_form_administration_unit_code_entry,
                    recipe_code_parsed,
                    COALESCE(package_size_parsed, package_size_entry) as package_size,
                    -- referring back from which type the text field was (in original.drug column text)
                    text_type_parsed,
                    -- raw values of dose because they can carry maybe some useful information (dose in mg for example)
                    dose_quantity_value_parsed as dose_quantity_value_raw_parsed,
                    dose_quantity_unit_parsed as dose_quantity_unit_raw_parsed,
                    dose_quantity_value_entry as dose_quantity_value_raw_entry,
                    dose_quantity_unit_entry as dose_quantity_unit_raw_entry,
                    -- based on which column matching was this row obtained
                    match_description
                from {source}
            )
            -- add RXNORM concept id
            select
                d.*, omop.concept_id as rxnorm_concept_id
            from final_drug_cleaned d
            left join {atc_to_omop} omop on
            d.drug_code = omop.source_value;
                            
            reset role;
            
            -- do not need atc_to_omop table anymore
            drop table if exists {atc_to_omop};     
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.schema),
                source=sql.Identifier(self.source_table),
                target=sql.Identifier(self.target_table),
                atc_to_omop=atc_to_omop_map,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
            set search_path to {schema};
            SELECT COUNT(*) FROM {target}
            """
            ).format(schema=sql.Identifier(self.schema), target=sql.Identifier(self.target_table))
        )

        with open(str(self.output_file), "a") as log_file:
            log_file.write(
                'Step04: Final table "' + str(self.target_table) + '" has ' + str(cur.fetchone()[0]) + " rows. \n"
            )

        cur.close()
        conn.close()
        self.mark_as_complete()


class CreateATCtoOMOP(CDASubtask):

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")  # This should be mapping
    target_table = luigi.Parameter(default="atc_to_omop")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    luigi_target_name = "create_atc_to_omop_table"
    target_tables = ["atc_to_omop"]

    def run(self):
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};
            
            drop table if exists {schema}.{atc_table};
            create table {schema}.{atc_table}
            (
                source_value      varchar(20),
                source_concept_id integer,
                concept_id        integer
            );
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                atc_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        data_folder = os.path.join(os.path.dirname(__file__), "data/")
        csv_file = data_folder + "hwisc_epi_ohdsithon_atc_to_omop.csv"
        import_csv_file(conn, csv_file, str(self.target_schema), str(self.target_table))

        cur.close()
        conn.close()
        self.mark_as_complete()
