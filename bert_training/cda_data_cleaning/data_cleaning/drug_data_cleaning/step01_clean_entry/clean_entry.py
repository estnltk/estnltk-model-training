import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class CleanEntry(CDASubtask):
    """
    1. Create an empty target table in the right role, then reset role
    2. Clean original.drug_entry, colmuns that are cleaned are
         - 'drug_code_display_name': (otherwise known as active ingredient),
           more detailed information in step00_create_cleaning_functions
         - 'effective_time': unifying and converting from varchar to timestamp
         - 'dose_quantity_value'/'rate_quantity_value': replace ',' with '.'
         - removing leading and trailing whitespaces
       Unifying drug_code_display_name which is sometimes in estonian and sometimes in latin
         - in order to unify it 3 new columns are added
            * 'active_ingredient_est', 'active_ingredient_latin', 'active_ingredient_eng'
         - the initial value is kept under column 'active_ingredient_raw'
         - as for OMOP mapping estonian names are most useful, column 'active_ingredient' contains estonian names
             if those are available, if not then the initial value 'active_ingredient_raw',
             so later the matching of entry and parsed can be done based on 'active_ingredient'

    Requirement: table 'active_substances' must exist in classifications schema. Needed for standardizing active ingredient
    and giving latin, estonian and english name.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter(default="drug_entry")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="drug_entry_cleaned")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    source_tables = ["drug_entry"]
    target_tables = ["drug_entry_cleaned"]

    def run(self):
        self.log_current_action("Step 2 clean entry")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        source_table = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.source_schema), table=sql.Identifier(self.source_table)
        )
        target_table = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.target_schema), table=sql.Identifier(self.target_table)
        )

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {target};
            create table {target}
            (
                id bigint,
                epi_id text,
                epi_type text,
                drug_id bigint,
                prescription_type text,
                effective_time timestamp with time zone,
                active_ingredient text,
                active_ingredient_est text,
                active_ingredient_latin text,
                active_ingredient_eng text,
                dose_quantity_value text,
                dose_quantity_unit text,
                rate_quantity_value text,
                rate_quantity_unit text,
                drug_form_administration_unit_code text,
                drug_form_administration_unit_code_display_name text,
                drug_code text,
                dose_quantity_extra text,
                package_size text,
                active_ingredient_raw text
            );

            reset role;
            """
            ).format(role=sql.Identifier(self.role), target=target_table)
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                r"""      
            set search_path to {schema};
                  
            with drug_entry_with_cleaned_active_ingredient as (
                -- clean active ingredient column using cleaning function created is step00_create_cleaning_functions
                select id,
                       epi_id,
                       epi_type,
                       drug_id,
                       prescription_type,
                       effective_time,
                       drug_code_display_name, -- can contain information in both latin and estonian
                       -- first element is cleaned active ingredient, second is drug_dose and third is package_size
                       -- keep in mind that indexing in postgresql starts from 1 and not 0
                       {schema}.clean_active_ingredient_entry(drug_code_display_name, drug_code) as active_ingredient_array,
                       -- first element is dose_quantity_value,
                       -- second element is dose_quantity_unit if it has accidentally been placed under dose_quantity_value, NULL otherwise
                       {schema}.clean_dose_quantity_value(dose_quantity_value) as dose_quantity_value_array,
                       dose_quantity_unit,
                       rate_quantity_value,
                       rate_quantity_unit,
                       drug_form_administration_unit_code,
                       drug_form_administration_unit_code_display_name,
                       drug_code
                from {source}
            )
            -- clean the rest of the columns
            -- add estonian, latin and english name of active ingredient
            insert
            into {target} (id,
                           epi_id,
                           epi_type,
                           drug_id,
                           prescription_type,
                           effective_time,
                           active_ingredient,
                           active_ingredient_est,
                           active_ingredient_latin,
                           active_ingredient_eng,
                           dose_quantity_value,
                           dose_quantity_unit,
                           rate_quantity_value,
                           rate_quantity_unit,
                           drug_form_administration_unit_code,
                           drug_form_administration_unit_code_display_name,
                           drug_code,
                           dose_quantity_extra,
                           package_size,
                           active_ingredient_raw)
            select id,
                   epi_id,
                   epi_type,
                   drug_id,
                   prescription_type,
                   case
                       when effective_time ~ '^\d{{4}}\d{{2}}\d{{2}}\d{{2}}\d{{2}}\d{{2}}$' then
                           to_timestamp(effective_time, 'YYYYMMDDHH24MISS')
                       when effective_time ~ '^\d{{4}}\d{{2}}\d{{2}}$' then
                           to_date(effective_time, 'YYYYMMDD')
                       when effective_time ~ '^\d{{2}}.\d{{2}}.\d{{4}}$' then
                           to_date(effective_time, 'DD MM YYYY')
                       when effective_time ~ '^\d{{4}}.\d{{2}}.\d{{2}}$' then
                           to_date(effective_time, 'YYYY MM DD')
                       when effective_time ~ '^\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}$' then
                           to_timestamp(effective_time, 'YYYY MM DD HH24:MI:SS')
                       else null
                       end                                                                         as effective_time,
                   -- estonian name of active_ingredient, if it does not exist then use the cleaned drug_code_display_name value
                   case when sub2.name is not null then lower(sub2.name)
                       else active_ingredient_array[1]
                   end                                                                             as active_ingredient,
                   -- if active_ingredient is in latin    -> adds estonian name to column active_ingredient_est
                   --                                          -> copys active_ingredient to active_ingredient_latin
                   case
                       when sub2.name is not null and sub1.latin_name is null then lower(sub2.name)
                       when sub1.latin_name is not null then lower(active_ingredient_array[1]) end as active_ingredient_est,
                   -- if active_ingredient is in estonian -> adds latin name to column active_ingredient_latin
                   --                                          -> copys active_ingredient to active_ingredient_est
                   case
                       when sub1.latin_name is not null then lower(sub1.latin_name)
                       when sub2.name is not null then lower(active_ingredient_array[1])
                       end                                                                         as active_ingredient_latin,
                   -- english name can come from either sub1 or sub2
                   case
                       when sub2.name is not null then lower(sub2.english_name)
                       else lower(sub1.english_name) end                                           as active_ingredient_eng,
                   -- cleaning was done inside cleaning function
                   dose_quantity_value_array[1] as dose_quantity_value,
                   case
                       when dose_quantity_value_array[2] is not null then dose_quantity_value_array[2] -- unit was under value
                       when dose_quantity_unit = '' then NULL
                       else trim(from dose_quantity_unit)
                       end                                                                         as dose_quantity_unit,
                   case
                       when rate_quantity_value = '' then NULL
                       else trim(from rate_quantity_value)
                       end                                                                         as rate_quantity_value,
                   case
                       when rate_quantity_unit = '' then NULL
                       else trim(from rate_quantity_unit)
                       end                                                                         as rate_quantity_unit,
                   case
                       when drug_form_administration_unit_code = '' then NULL
                       else trim(from drug_form_administration_unit_code)
                       end                                                                         as drug_form_administration_unit_code,
                   case
                       when drug_form_administration_unit_code_display_name = '' then NULL
                       else lower(trim(from drug_form_administration_unit_code_display_name))
                       end                                                                         as drug_form_administration_unit_code_display_name,
                   case
                       when drug_code = '' then NULL
                       else trim(from drug_code)
                       end                                                                         as drug_code,
            
                   active_ingredient_array[2]                                                      as dose_quantity_extra,
                   active_ingredient_array[3]                                                      as package_size,
                   drug_code_display_name                                                          as active_ingredient_raw
            from drug_entry_with_cleaned_active_ingredient as de
                     -- add estonian, english and latin ingredient name
                     left join classifications.active_substances as sub1
                               on lower(active_ingredient_array[1]) = lower(sub1.name)
                     left join classifications.active_substances as sub2
                               on lower(active_ingredient_array[1]) = lower(sub2.latin_name);
            """
            ).format(schema=sql.Identifier(self.target_schema), source=source_table, target=target_table)
        )
        conn.commit()
        conn.close()
        self.mark_as_complete()


class LogCleaningResults(CDASubtask):
    """
    1. Cleaning and unifying tables
         * entry (original.drug_entry)
         * parised (*_drug_parsed, obtained in step1 from original.drug text field)
    2. Mapping missing drug codes to parsed table
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    entry_source_table = luigi.Parameter(default="drug_entry_cleaned")
    parsed_source_table = luigi.Parameter(default="drug_parsed_cleaned")
    output_file = luigi.Parameter(default="")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()
    luigi_target_name = "clean_entry_and_parsed_table"

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Size of cleaned entry table
        cur.execute(
            sql.SQL(
                """
            set search_path to {schema};
            SELECT COUNT(*) FROM {target}
            """
            ).format(schema=sql.Identifier(self.schema), target=sql.Identifier(self.entry_source_table))
        )
        entry_size = str(cur.fetchone()[0])

        # Size of cleaned parsed table
        cur.execute(
            sql.SQL(
                """
            set search_path to {schema};
            SELECT COUNT(*) FROM {target}
            """
            ).format(schema=sql.Identifier(self.schema), target=sql.Identifier(self.parsed_source_table))
        )
        parsed_size = str(cur.fetchone()[0])

        # missing drug_codes
        cur.execute(
            sql.SQL(
                """
            set search_path to {schema};
            SELECT COUNT(*) FROM {target} 
            where drug_code is NULL;
            """
            ).format(schema=sql.Identifier(self.schema), target=sql.Identifier(self.parsed_source_table))
        )
        count_missing_drug_code = str(cur.fetchone()[0])

        # missing ingredients
        cur.execute(
            sql.SQL(
                """
            set search_path to {schema};
            SELECT COUNT(*) FROM {target}
            where active_ingredient is NULL;
            """
            ).format(schema=sql.Identifier(self.schema), target=sql.Identifier(self.parsed_source_table))
        )
        count_missing_active_ingredients = str(cur.fetchone()[0])

        with open(str(self.output_file), "a") as log_file:
            log_file.write(
                'Step02: Cleaned entry table "' + str(self.entry_source_table) + '" has ' + entry_size + " rows. \n"
            )
            log_file.write(
                'Step02: Cleaned parsed table "' + str(self.parsed_source_table) + '" has ' + parsed_size + " rows. \n"
            )
            log_file.write("  Before mapping missing data in parsed: \n")
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
