import os
import luigi
from psycopg2 import sql

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask


class ValidateResults(CDASubtask):

    config_file = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="drug_cleaned")
    output_file = luigi.Parameter(default="validation_output")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Validate results")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # contains sql code and description of the sql code
        sqls_list = {
            """Links to source code (each row has at least one of id_entry or id_parsed), expected to be 0
            """: """
                -- Links to source are preserved (expected to be 0)
                select count(*) from {schema}.{target}
                where id_entry is null and id_parsed is null;
            """,
            """Links to source code (each parsed row has id_original_drug which directs back to the original table) expected to be 0
            """: """                   
                select count(*) from {schema}.{target}
                where id_original_drug is null and id_parsed is not null;
            """,
            """Links to source code (each entry row does not have id_original_drug) expected to be 0
            """: """
               select count(*) from {schema}.{target}
               where id_original_drug is not null and id_parsed is null;
            """,
            """------------------------------------------------------------------------------------
            All different epi_types""": """
                select epi_type, count(*) from {schema}.{target}
                group by epi_type;
            """,
            """------------------------------------------------------------------------------------
            All different effective_times""": """                
                select year, count(*) from
                (
                    select extract(year from effective_time) as year from {schema}.{target}
                ) as tbl
                group by year;
            """,
            """
            In which units is effective_time measured in (hours, minutes or seconds)
            """: """                                   
                select hours, minutes, seconds, count(*) from
                (
                    select
                        extract(hour from effective_time)!= 0 as hours,
                        extract(minute from effective_time)!= 0 as minutes,
                        extract(second from effective_time)!= 0 as seconds
                    from {schema}.{target}
                ) as tbl
                group by hours, minutes, seconds
                order by hours, minutes, seconds;
            """,
            """------------------------------------------------------------------------------------
            List of drug codes that have multiple different active ingredients
            This can be used to improve cleaning and standardizing active ingredients
            e.g. A03BB01,butüülskopolamiin
                  A03BB01,Butylscopolaminum"
            Complete list of problems:""": """                               
                select drug_code, active_ingredient, count(*) as row_count
                from {schema}.{target}
                where drug_code in
                (
                    select drug_code from
                    (
                        select drug_code, active_ingredient, count(*) as row_count
                        from {schema}.{target}
                        where drug_code is not null or active_ingredient is not null
                        group by drug_code, active_ingredient
                    ) as tbl
                    where drug_code is not null
                    group by drug_code
                    having count(*) > 1
                )
                group by drug_code, active_ingredient
                order by drug_code, active_ingredient;
            """,
            """
            Look for drug code assignment failures. There are some. These could be resolved manually
            This could be separate workflow step
            """: """
                select active_ingredient, drug_name_parsed, count(*) from {schema}.{target}
                where drug_code is null and active_ingredient is not null
                group by active_ingredient, drug_name_parsed
                order by active_ingredient, drug_name_parsed;
            """,
            """-------------------------------------------------------------------------------------------------
            Rows where there is no drug name""": """    
                select active_ingredient, count(*) from {schema}.{target}
                where drug_code is null and active_ingredient is not null and drug_name_parsed is null
                group by active_ingredient;
            """,
            "Different possible drug names": """
                select drug_name_parsed, count(*) from {schema}.{target}
                where drug_name_parsed is not null
                group by drug_name_parsed;
            """,
            """
            Some drug names do not get a drug code. This could be again in improvement step. Though drug name is not used
            in matching so not very important
            """: """
                select drug_name_parsed, count(*) from {schema}.{target}
                where drug_name_parsed is not null and drug_code is null
                group by drug_name_parsed
                order by count desc;
            """,
            """                        
            A drug name should not have several drug codes. Again, drug name is not used in matching, so there has been no cleaning
            or mapping improvements done
            example:
            PARACETAMOL/CODEINE VITABALANS	N02AA80	7
            PARACETAMOL/CODEINE VITABALANS	N02AJ06	1
            PARACETAMOL/CODEINE VITABALANS	N02BE92	3
            https://rx.ee/otsi?searchword=N02AA80&ordering=&searchphrase=all
            N02AA80 ok -- Invalid ATC code in global ATC index
            N02AJ06 ok
            N02BE92 not ok
            Resolution some ATC codes have been invalidated during years. Hence a drug can have several ATC codes
             Some enginuity is needed to resolve this issue
            """: """
                select drug_name_parsed, drug_code, count(*) as row_count
                from {schema}.{target}
                where drug_name_parsed in
                (
                    select drug_name_parsed from
                    (
                        select drug_name_parsed, drug_code, count(*) as row_count  from {schema}.{target}
                        where drug_name_parsed is not null and drug_code is not null
                        group by drug_name_parsed, drug_code
                    ) as tbl
                    group by drug_name_parsed
                    having count(*) > 1
                )
                group by drug_name_parsed, drug_code
                order by drug_name_parsed, drug_code;
            """,
            """
            List of drug names without active ingredient
            """: """      
                select drug_name_parsed, count(*) as row_count
                from {schema}.{target}
                where drug_name_parsed is not null
                  and active_ingredient is null
                group by drug_name_parsed
                order by row_count desc;
            """,
            """
            Drug name should not lead to  sevaral variants of active ingredients
            main cause is that active ingredients consisting of two ingredients (e.g. salmeterolum+fluticasonum,salmeterool+flutikasoon)
            are not standardized because they do not exist in classification.active_substance table
            """: """
                select drug_name_parsed, active_ingredient, count(*) as row_count
                from {schema}.{target}
                where drug_name_parsed in
                      (
                          select drug_name_parsed
                          from (
                                   select drug_name_parsed, active_ingredient, count(*) as row_count
                                   from {schema}.{target}
                                   where drug_name_parsed is not null
                                     and active_ingredient is not null
                                   group by drug_name_parsed, active_ingredient
                               ) as tbl
                          group by drug_name_parsed
                          having count(*) > 1
                      )
                group by drug_name_parsed, active_ingredient
                order by drug_name_parsed, active_ingredient;
            """,
            """------------------------------------------------------------------------------------
            Different dose quantity values""": """                   
                select dose_quantity_value, count(*)
                from {schema}.{target}
                where dose_quantity_value is not null
                group by dose_quantity_value;       
            """,
            """
            Non-numeric dose quantity values
            """: """
                select array_agg(distinct dose_quantity_value), count(*)
                from {schema}.{target}
                where dose_quantity_value ~ '[:alpha:]';
            """,
            """
            Different dose quantity units
            Ideally fields like '1 annus 2 X päevas' should be split and placed under rate quantity value and rate quantity unit
            """: """
                select dose_quantity_unit, count(*)
                from {schema}.{target}
                where dose_quantity_unit is not null
                group by dose_quantity_unit;
            """,
            """------------------------------------------------------------------------------------
            Different rate quantity values""": """
                select rate_quantity_value, count(*)
                from {schema}.{target}
                where rate_quantity_value is not null
                group by rate_quantity_value;
            """,
            """
            Non-numeric rate quantity values
            """: """
                select array_agg(distinct rate_quantity_value), count(*)
                from {schema}.{target}
                where rate_quantity_value ~ '[:alpha:]';
            """,
            """
            Different rate quantity units
            """: """
                select rate_quantity_unit, count(*)
                from {schema}.{target}
                where rate_quantity_unit is not null
                group by rate_quantity_unit;
            """,
            """------------------------------------------------------------------------------------
            Irrelevant stuff, column drug_form_administration_unit_code_entry
            This contains some magic stuff but it is irrelevant""": """
            select drug_form_administration_unit_code_entry, count(*)
            from {schema}.{target}
            where drug_form_administration_unit_code_entry is not null
            group by drug_form_administration_unit_code_entry;
            """,
            """
            Different drug_form_administration_unit_code_entry values
            """: """
                select drug_form_administration_unit_code_entry, count(*)
                from {schema}.{target}
                where drug_form_administration_unit_code_entry ~ '[0-9]'
                group by drug_form_administration_unit_code_entry
                order by count;
            """,
            """------------------------------------------------------------------------------------
            Number of prescription types that are not PRE or CURE""": """
                select array_agg(prescription_type_entry), count(*)
                from {schema}.{target}
                where prescription_type_entry not in ('PRE', 'CURE');
            """,
            """------------------------------------------------------------------------------------
            Non-numeric recipe codes""": """
                select count(*)
                from {schema}.{target}
                where not recipe_code_parsed ~ '[0-9]+'
                  and recipe_code_parsed is not null
                  and recipe_code_parsed != '';
            """,
            """------------------------------------------------------------------------------------
            Different package sizes""": """
                select package_size, count(*)
                from {schema}.{target}
                where package_size is not null
                group by package_size
                order by count;
            """,
            """------------------------------------------------------------------------------------
            Different rxnorm concept id-s""": """
                select rxnorm_concept_id, count(*)
                from {schema}.{target}
                where rxnorm_concept_id is not null
                group by rxnorm_concept_id
                order by count desc;
            """,
            """------------------------------------------------------------------------------------
            Different dosage forms""": """
                select dosage_form, count(*)
                from {schema}.{target}
                where dosage_form is not null
                group by dosage_form
                order by count desc;
            """,
        }

        for desc, sql_code in sqls_list.items():
            cur.execute(
                sql.SQL(sql_code).format(schema=sql.Identifier(self.schema), target=sql.Identifier(self.source_table))
            )

            col_names_str = ",".join([i[0] for i in cur.description])  # sql output column names as string
            output_as_str = "\n".join(map(str, cur.fetchall()))  # sql output as string

            file_path = os.path.join(os.path.dirname(__file__), "output/" + str(self.output_file))
            with open(file_path, "a") as log_file:
                log_file.write(
                    "-------------------------------------------------------------------------------------\n"
                    + desc
                    + "\n\n"
                    + col_names_str
                    + "\n"
                    + str(output_as_str)
                    + "\n\n"
                )

        self.mark_as_complete()
