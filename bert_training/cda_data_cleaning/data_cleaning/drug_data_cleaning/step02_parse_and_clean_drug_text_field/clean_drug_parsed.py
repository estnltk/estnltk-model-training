import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class CleanParsed(CDASubtask):
    """
    1. Create empty table in the right role
    2. Clean columns by
        - replacing empty strings with NULL
        - in dose_quantity_value/rate_quantity_value replace ',' with '.'
        - in atc_code remove trailing '-' and ','
        Unifying active_ingredient which is sometimes in estonian and sometimes in latin
         - in order to unify it 3 new columns are added
            * 'active_ingredient_est', 'active_ingredient_latin', 'active_ingredient_eng'
         - the initial value is kept under column 'active_ingredient_raw'
         - as for OMOP mapping estonian names are most useful, column 'active_ingredient' contains estonian names
             if those are available, if not then the initial value 'active_ingredient_raw',
             so later the matching of entry and parsed can be done based on 'active_ingredient'

    3. Change date type from varchar to timestamp in column effective_time
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="drug_parsed")
    target_table = luigi.Parameter(default="drug_parsed_cleaned")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    luigi_target_name = "clean_parsed_table"
    source_tables = ["drug_parsed"]
    target_tables = ["drug_parsed_cleaned"]

    def run(self):
        self.log_current_action("Step 2 clean parsed")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                r"""
            set role {role};
            set search_path to {schema};
            
            drop table if exists {target_parsed};
            create table {target_parsed}
            (
                id bigint,
                id_original_drug bigint,
                epi_id text,
                epi_type text,
                effective_time text,
                dose_quantity_value text,
                dose_quantity_unit text,
                rate_quantity_value text,
                rate_quantity_unit text,
                drug_form_administration_unit_code_display_name text,
                drug_code text,
                active_ingredient text,
                active_ingredient_est text,
                active_ingredient_latin text,
                active_ingredient_eng text,
                active_ingredient_raw text,
                drug_name text,
                recipe_code text,
                package_size text,
                text_type double precision
            );
            
            reset role;
        """
            ).format(
                schema=sql.Identifier(self.schema),
                role=sql.Identifier(self.role),
                target_parsed=sql.Identifier(self.target_table),
            )
        )

        conn.commit()

        cur.execute(
            sql.SQL(
                """ 
            insert into {target_parsed} (id,
                                  id_original_drug,
                                  epi_id,
                                  epi_type,
                                  effective_time,
                                  dose_quantity_value,
                                  dose_quantity_unit,
                                  rate_quantity_value,
                                  rate_quantity_unit,
                                  drug_form_administration_unit_code_display_name,
                                  drug_code,
                                  active_ingredient,
                                  active_ingredient_est,
                                  active_ingredient_latin,
                                  active_ingredient_eng,
                                  active_ingredient_raw,
                                  drug_name,
                                  recipe_code,
                                  package_size,
                                  text_type)
            select row_number() over ()                                                   as id,               -- unique id for each row
                   id                                                                     as id_original_drug, -- corresponding id in the original.drug table
                   epi_id,
                   epi_type,
                   case
                       when date = '' then NULL
                       else regexp_replace(date, ',|\s+', '', 'g')
                       end                                                                as effective_time,
                   case
                       when dose_quantity = '' then NULL
                       else trim(from regexp_replace(dose_quantity, ',', '.'))
                       end                                                                as dose_quantity_value,
                   case
                       when dose_quantity_unit = '' then NULL
                       else lower(trim(from dose_quantity_unit))
                       end                                                                as dose_quantity_unit,
                   case
                       when rate_quantity = '' then NULL
                       else trim(from regexp_replace(rate_quantity, ',', '.'))
                       end                                                                as rate_quantity_value,
                   case
                       when rate_quantity_unit = '' then NULL
                       else trim(from rate_quantity_unit)
                       end                                                                as rate_quantity_unit,
                   case
                       when drug_form_administration_unit_code_display_name = '' then NULL
                       else lower(trim(from drug_form_administration_unit_code_display_name))
                       end                                                                as drug_form_administration_unit_code_display_name,
                   case
                       when atc_code = '' then NULL
                       else trim(from regexp_replace(atc_code, ',|-| ', '', 'g'))
                       end                                                                as drug_code,
                   -- estonian name of active_ingredient, if it does not exist then use the raw active_ingredient value
                   case
                       when sub2.name is not null then lower(sub2.name)
                       when active_ingredient != '' then lower(trim(active_ingredient))
                       end                                                                as active_ingredient,
                   -- if active_ingredient is in latin    -> adds estonian name to column active_ingredient_est
                   --                                          -> copys active_ingredient to active_ingredient_latin
                   case
                       when sub2.name is not null and sub1.latin_name is null then lower(sub2.name)
                       when sub1.latin_name is not null then lower(active_ingredient) end as active_ingredient_est,
                   -- if active_ingredient is in estonian -> adds latin name to column active_ingredient_latin
                   --                                          -> copys active_ingredient to active_ingredient_est
                   case
                       when sub1.latin_name is not null then lower(sub1.latin_name)
                       when sub2.name is not null then lower(active_ingredient)
                       end                                                                as active_ingredient_latin,
                   -- english name can come from either sub1 or sub2
                   case
                       when sub2.name is not null then lower(sub2.english_name)
                       else lower(sub1.english_name) end                                  as active_ingredient_eng,
                   active_ingredient                                                      as active_ingredient_raw,
            
                   case
                       when drug_name = 'Ravimi nimetus' or drug_name = '' then NULL
                       else trim(from drug_name)
                       end                                                                as drug_name,
                   case when recipe_code = '' then NULL 
                        else recipe_code
                        end                                                               as  recipe_code,
                   case when package_size = '' then NULL
                        else package_size
                        end                                                               as package_size,
                   text_type
            from {source_parsed}
                     -- add estonian, english and latin ingredient name
                     left join classifications.active_substances as sub1
                               on lower(active_ingredient) = lower(sub1.name)
                     left join classifications.active_substances as sub2
                               on lower(active_ingredient) = lower(sub2.latin_name);
            
            -- 2.
            -- unify date column
            update {target_parsed}
            set effective_time =
                case when effective_time ~ '^\d{{4}}\d{{2}}\d{{2}}\d{{2}}\d{{2}}\d{{2}}$' then
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
                end;

            -- change effective time type from text to timestamp
            alter table {target_parsed}
            alter column effective_time  TYPE timestamp using effective_time::timestamp without time zone;
            
        """
            ).format(
                schema=sql.Identifier(self.schema),
                source_parsed=sql.Identifier(self.source_table),
                target_parsed=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
