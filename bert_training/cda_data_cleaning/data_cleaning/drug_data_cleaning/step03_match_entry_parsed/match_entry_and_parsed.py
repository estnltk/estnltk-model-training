import os
import luigi

from psycopg2 import sql
from typing import Union, TYPE_CHECKING

from cda_data_cleaning.common.luigi_tasks import CDASubtask
from cda_data_cleaning.common.luigi_tasks import CDABatchTask


class MatchEntryParsed(CDABatchTask):
    """
    TODO: Explain why you do these steps not what steps you do?

    Matching tables '*_entry_cleaned' and '*_parsed_cleaned' consists of 12 steps.
    First 0-10 steps produce matched table '*_drug_matched'
    Step 11 removes duplicates creating during matching and produces table '*_drug_matched_wo_duplicates'.

    Steps
        0. Step0CreateUnresolvedTables:
           Create two tables '*_unresolved_entry', '*_unresolved_parsed'
           Those tables contain the id-s of unresolved (not matched) rows. Those are the rows we are going to work with.
        1. Step1Match_E_DC_DOV_DOU_RAV_RAU_ING_PT:
           Match based on epi_id, drug_code, dose_quantity_value, dose_quantity_unit,rate_quantity_value,rate_quantity_unit,
           active_ingredient, package_type. Insert them to a new empty table '*_drug_matched'.
        2. Step2AddUnique:
           Insert to '*_drug_matched' epicrisis that only exist in either parsed or entry to (unique rows). Decrease unresolved.
        3. Step3MatchE_DC_ING_DOV_DOU:
           Match based on epi_id, drug_code, dose_quantity_value, dose_quantity_unit, active_ingredient.
           Insert them to a new table '*_drug_matched'. Decrease unresolved.
        4. Step4Match_E_DC_ING_PT:
           Match based on  epi_id, drug_code, active_ingredient, package_type.
           Insert them to a new table '*_drug_matched'. Decrease unresolved.
        5. Step5Match_E_DC_ING:
           Match based on  epi_id, drug_code, active_ingredient.
           Insert them to a new table '*_drug_matched'. Decrease unresolved.
        6. Step6Match_E_ING_PT:
           Match based on  epi_id, active_ingredient, package_type
           Insert them to a new table '*_drug_matched'. Decrease unresolved.
        7. Step7Match_E_ING:
           Match based on  epi_id, active_ingredient.
           Insert them to a new table '*_drug_matched'. Decrease unresolved.
        8. Step8Match_E_DC:
           Match based on  epi_id, drug_code.
           Insert them to a new table '*_drug_matched'. Decrease unresolved.
        9. Step9AddUnique2:
           From all the remaining unresolved rows, insert to '*_drug_matched' epicirsis that only exist in
           either parsed or entry (unique rows). Decrease unresolved.
        10. Step10AddUnresolved:
            Add the remaining unresolved rows to '*_drug_matched'.
        11. Delete duplicates
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    text_source_table = luigi.Parameter(default="drug_parsed_cleaned")
    entry_source_table = luigi.Parameter(default="drug_entry_cleaned")
    matched_table = luigi.Parameter(default="drug_matched")
    matched_wo_duplicates_table = luigi.Parameter(default="drug_matched_wo_duplicates")
    table_prefix = luigi.Parameter(default="")
    output_file = luigi.Parameter(default="")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    luigi_target_name = "match_entry_parsed_tables"

    def requires(self):

        task_01 = Step0CreateUnresolvedTables(parent_task=self, requirement=self.requirement)
        task_02 = Step1Match_E_DC_DOV_DOU_RAV_RAU_ING_PT(parent_task=self, requirement=task_01)
        task_03 = Step2AddUnique(parent_task=self, requirement=task_02)
        task_04 = Step3MatchE_DC_ING_DOV_DOU(parent_task=self, requirement=task_03)
        task_05 = Step4Match_E_DC_ING_PT(parent_task=self, requirement=task_04)
        task_06 = Step5Match_E_DC_ING(parent_task=self, requirement=task_05)
        task_07 = Step6Match_E_ING_PT(parent_task=self, requirement=task_06)
        task_08 = Step7Match_E_ING(parent_task=self, requirement=task_07)
        task_09 = Step8Match_E_DC(parent_task=self, requirement=task_08)
        task_10 = Step9AddUnique2(parent_task=self, requirement=task_09)
        task_11 = Step10AddUnresolved(parent_task=self, requirement=task_10)
        task_12 = Step11DeleteDuplicates(parent_task=self, requirement=task_11)
        return [
            task_01,
            task_02,
            task_03,
            task_04,
            task_05,
            task_06,
            task_07,
            task_08,
            task_09,
            task_10,
            task_11,
            task_12,
        ]

    def run(self):
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            select count(*) from {schema}.{target};
            """
            ).format(schema=sql.Identifier(self.schema), target=sql.Identifier(self.matched_wo_duplicates_table))
        )

        # write information of table size to log file
        with open(str(self.output_file), "a") as log_file:
            log_file.write(
                'Step03: Matched table (without duplicates) "'
                + str(self.matched_wo_duplicates_table)
                + '" has '
                + str(cur.fetchone()[0])
                + " rows \n"
            )

        cur.close()
        conn.close()
        self.mark_as_complete()


class CDAMatchingSubtask(CDASubtask):
    """
    A separate class for keeping the table names consistent throughout the entire matching step.

    The number of different temporal and non-temporal tables is to big to handle them as luigi parameters.
    Instead, the class uses a simple mechanical rules to derive them from the parameters of a parent class.

    Each subclass should be decorated with metainfo about its database usage and modification patterns.
    """

    # This must be here or luigi goes crazy
    parent_task = luigi.TaskParameter()

    class DatabaseTables:
        __slots__ = [
            "source_entry",
            "source_text",
            "matched",
            "matched_wo_duplicates",
            "unresolved_text_rows",
            "unresolved_entry_rows",
        ]

        # needed because otherwise mypy complains
        #   - error: "DatabaseTables" has no attribute "matched" etc
        if TYPE_CHECKING:
            source_entry = None  # type: str
            source_text = None  # type: str
            matched = None  # type: str
            matched_wo_duplicates = None  # type: str
            unresolved_text_rows = None  # type: str
            unresolved_entry_rows = None  # type: str

    def __init__(self, parent_task: MatchEntryParsed, requirement: Union[luigi.TaskParameter, "CDAMatchingSubtask"]):
        super().__init__(parent_task=parent_task, requirement=requirement)

        # Propagate parameters from the parent task
        self.config_file = parent_task.config_file
        self.role = parent_task.role
        self.schema = parent_task.schema
        self.text_source_table = parent_task.text_source_table
        self.entry_source_table = parent_task.entry_source_table
        self.table_prefix = str(parent_task.table_prefix)
        self.luigi_targets_folder = parent_task.luigi_targets_folder

        self.tables = self.DatabaseTables()

        # Permanent tables
        self.tables.source_entry = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.entry_source_table)
        )
        self.tables.source_text = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.text_source_table)
        )
        self.tables.matched = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.matched_table)
        )
        self.tables.matched = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.matched_table)
        )
        self.tables.matched_wo_duplicates = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(str(parent_task.matched_table) + "_wo_duplicates")
        )

        # Temporary tables for unresolved rows
        self.tables.unresolved_text_rows = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "unresolved_text")
        )
        self.tables.unresolved_entry_rows = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "unresolved_entry")
        )

    def update_unresolved_rows(self, cur):
        """
        Updates the list of unresolved html and entry rows.
        By construction corresponding tables always decrease after some rows are added to matched table.
        """

        cur.execute(
            sql.SQL(
                """
            delete from {unmatched_entry}
            where id in (select id_entry from {matched});

            delete from {unmatched_text}
            where id in (select id_parsed from {matched});
            """
            ).format(
                matched=self.tables.matched,
                unmatched_text=self.tables.unresolved_text_rows,
                unmatched_entry=self.tables.unresolved_entry_rows,
            )
        )
        cur.connection.commit()


class Step0CreateUnresolvedTables(CDAMatchingSubtask):

    luigi_target_name = "Step1CreateUnresolvedTables_done"

    def run(self):
        self.log_current_action("Matching 0")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # TODO: Simplify sql as naming is unnecessary
        cur.execute(
            sql.SQL(
                """
            set role {role};
            
            -- all '*_drug_entry' id's where epi_id exists BOTH in '*_drug_parsed' and '*_drug_entry' and require matching
            -- the rest are unique for '*_drug_entry' and therefore do not need matching
            drop table if exists {un_entry};
            create table {un_entry} as
            select id
            from {source_entry} 
            where epi_id in (select epi_id from {source_parsed});
            
            -- all '*_drug_parsed' id's where epi_id exists BOTH in '*_drug_parsed' and '*_drug_entry' and require matching
            -- the rest are unique for '*_drug_parsed' and therefore do not need matching
            drop table if exists {un_parsed};
            create table  {un_parsed} as
            select id
            from {source_parsed} 
            where epi_id in (select epi_id from {source_entry});
            """
            ).format(
                role=sql.Identifier(self.role),
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step1Match_E_DC_DOV_DOU_RAV_RAU_ING_PT(CDAMatchingSubtask):
    """
    Match based on epi_id, drug_code, dose_quantity_value, dose_quantity_unit,rate_quantity_value,rate_quantity_unit,
    active_ingredient, package_type.
    Insert them to a new empty table '*_drug_matched'.
    """

    luigi_target_name = "Step1Match_E_DC_DOV_DOU_RAV_RAU_ING_PT_done"
    source_tables = ["source_entry", "source_text"]
    target_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 1")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {target};
            create table {target} as
            select distinct de.id                                              as id_entry,
                            de.epi_id                                          as epi_id_entry,
                            de.epi_type                                        as epi_type_entry,
                            de.drug_id                                         as drug_id_entry,
                            de.prescription_type                               as prescription_type_entry,
                            de.effective_time                                  as effective_time_entry,
                            de.drug_code                                       as drug_code_entry,
                            de.active_ingredient                               as active_ingredient_entry,
                            de.active_ingredient_est                           as active_ingredient_est_entry,
                            de.active_ingredient_latin                         as active_ingredient_latin_entry,
                            de.active_ingredient_eng                           as active_ingredient_eng_entry,
                            de.dose_quantity_value                             as dose_quantity_value_entry,
                            de.dose_quantity_unit                              as dose_quantity_unit_entry,
                            de.rate_quantity_value                             as rate_quantity_value_entry,
                            de.rate_quantity_unit                              as rate_quantity_unit_entry,
                            de.drug_form_administration_unit_code              as drug_form_administration_unit_code_entry,
                            de.drug_form_administration_unit_code_display_name as dosage_form_entry,
                            de.dose_quantity_extra                             as dose_quantity_extra_entry,
                            de.package_size                                    as package_size_entry,
                            de.active_ingredient_raw                           as active_ingredient_raw_entry,
            
                            dp.id                                              as id_parsed,
                            dp.id_original_drug                                as id_original_drug_parsed,
                            dp.epi_id                                          as epi_id_parsed,
                            dp.epi_type                                        as epi_type_parsed,
                            dp.effective_time                                  as effective_time_parsed,
                            dp.drug_code                                       as drug_code_parsed,
                            dp.active_ingredient                               as active_ingredient_parsed,
                            dp.active_ingredient_est                           as active_ingredient_est_parsed,
                            dp.active_ingredient_latin                         as active_ingredient_latin_parsed,
                            dp.active_ingredient_eng                           as active_ingredient_eng_parsed,
                            dp.drug_name                                       as drug_name_parsed,
                            dp.dose_quantity_value                             as dose_quantity_value_parsed,
                            dp.dose_quantity_unit                              as dose_quantity_unit_parsed,
                            dp.rate_quantity_value                             as rate_quantity_value_parsed,
                            dp.rate_quantity_unit                              as rate_quantity_unit_parsed,
                            dp.drug_form_administration_unit_code_display_name as dosage_form_parsed,
                            dp.recipe_code                                     as recipe_code_parsed,
                            dp.package_size                                    as package_size_parsed,
                            dp.text_type                                       as text_type_parsed,
                            dp.active_ingredient_raw                           as active_ingredient_raw_parsed,
                            'epi_id, drug_code, active_ingredient,dose_quantity_value, dose_quantity_unit, rate_quantity_value,rate_quantity_unit, drug_form_administration_unit_code_display_name'
                                                                               as match_description
            from {source_entry} as de
                     join {source_parsed} as dp
                          on
                                  de.epi_id = dp.epi_id and
                                  de.drug_code = dp.drug_code and
                                  de.dose_quantity_value = dp.dose_quantity_value and
                                  de.dose_quantity_unit = dp.dose_quantity_unit and
                                  de.rate_quantity_value = dp.rate_quantity_value and
                                  de.rate_quantity_unit = dp.rate_quantity_unit and
                                  (de.active_ingredient like '%' || dp.active_ingredient || '%') and
                                  -- matches for example - parsed: silmatilgad, entry: silmatilgad, lahus
                                  de.drug_form_administration_unit_code_display_name like
                                  '%' || dp.drug_form_administration_unit_code_display_name || '%'
            where
               -- time is not contradicting 1. either both are NULL 2. one is NULL 3. both are equal
                de.effective_time is NULL
               or dp.effective_time is NULL
               or de.effective_time = dp.effective_time;
            """
            ).format(
                role=sql.Identifier(self.role),
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step2AddUnique(CDAMatchingSubtask):

    luigi_target_name = "Step2AddUnique_done"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 2")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};
            
            -- epicrisis that only exist in (are unique to) <prefix>_drug_cleaned_parsed table
            insert into {target} (id_parsed,
                                  id_original_drug_parsed,
                                  epi_id_parsed,
                                  epi_type_parsed,
                                  effective_time_parsed,
                                  drug_code_parsed,
                                  active_ingredient_parsed,
                                  active_ingredient_est_parsed,
                                  active_ingredient_latin_parsed,
                                  active_ingredient_eng_parsed,
                                  drug_name_parsed,
                                  dose_quantity_value_parsed,
                                  dose_quantity_unit_parsed,
                                  rate_quantity_value_parsed,
                                  rate_quantity_unit_parsed,
                                  dosage_form_parsed,
                                  recipe_code_parsed,
                                  package_size_parsed,
                                  text_type_parsed,
                                  active_ingredient_raw_parsed,
                                  match_description)
            select dp.id,
                   dp.id_original_drug,
                   dp.epi_id,
                   dp.epi_type,
                   dp.effective_time,
                   dp.drug_code,
                   dp.active_ingredient,
                   dp.active_ingredient_est,
                   dp.active_ingredient_latin,
                   dp.active_ingredient_eng,
                   dp.drug_name,
                   dp.dose_quantity_value,
                   dp.dose_quantity_unit,
                   dp.rate_quantity_value,
                   dp.rate_quantity_unit,
                   dp.drug_form_administration_unit_code_display_name,
                   dp.recipe_code,
                   dp.package_size,
                   dp.text_type,
                   dp.active_ingredient_raw,
                   'unique_parsed'
            from {source_parsed} dp
            where dp.id not in (select id from {un_parsed});

            -- epicrisis that only exist in (are unique to) <prefix>_drug_cleaned_entry table
            insert into {target}(id_entry,
                                 epi_id_entry,
                                 epi_type_entry,
                                 drug_id_entry,
                                 prescription_type_entry,
                                 effective_time_entry,
                                 drug_code_entry,
                                 active_ingredient_entry,
                                 active_ingredient_est_entry,
                                 active_ingredient_latin_entry,
                                 active_ingredient_eng_entry,
                                 dose_quantity_value_entry,
                                 dose_quantity_unit_entry,
                                 rate_quantity_value_entry,
                                 rate_quantity_unit_entry,
                                 drug_form_administration_unit_code_entry,
                                 dosage_form_entry,
                                 dose_quantity_extra_entry,
                                 package_size_entry,
                                 active_ingredient_raw_entry,
                                 match_description)
            select de.id,
                   de.epi_id,
                   de.epi_type,
                   de.drug_id,
                   de.prescription_type,
                   de.effective_time,
                   de.drug_code,
                   de.active_ingredient,
                   de.active_ingredient_est,
                   de.active_ingredient_latin,
                   de.active_ingredient_eng,
                   de.dose_quantity_value,
                   de.dose_quantity_unit,
                   de.rate_quantity_value,
                   de.rate_quantity_unit,
                   de.drug_form_administration_unit_code,
                   de.drug_form_administration_unit_code_display_name,
                   de.dose_quantity_extra,
                   de.package_size,
                   de.active_ingredient_raw,
                   'unique_entry' as match_description
            from {source_entry} de
            where de.id not in (select id from {un_entry});
            """
            ).format(
                role=sql.Identifier(self.role),
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step3MatchE_DC_ING_DOV_DOU(CDAMatchingSubtask):

    luigi_target_name = "Step3MatchE_DC_ING_DOV_DOU_done"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 3")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} (id_parsed,
                                  id_original_drug_parsed,
                                  epi_id_parsed,
                                  epi_type_parsed,
                                  effective_time_parsed,
                                  drug_code_parsed,
                                  active_ingredient_parsed,
                                  active_ingredient_est_parsed,
                                  active_ingredient_latin_parsed,
                                  active_ingredient_eng_parsed,
                                  drug_name_parsed,
                                  dose_quantity_value_parsed,
                                  dose_quantity_unit_parsed,
                                  rate_quantity_value_parsed,
                                  rate_quantity_unit_parsed,
                                  dosage_form_parsed,
                                  recipe_code_parsed,
                                  package_size_parsed,
                                  text_type_parsed,
                                  active_ingredient_raw_parsed,
                                  id_entry,
                                  epi_id_entry,
                                  epi_type_entry,
                                  drug_id_entry,
                                  prescription_type_entry,
                                  effective_time_entry,
                                  drug_code_entry,
                                  active_ingredient_entry,
                                  active_ingredient_est_entry,
                                  active_ingredient_latin_entry,
                                  active_ingredient_eng_entry,
                                  dose_quantity_value_entry,
                                  dose_quantity_unit_entry,
                                  rate_quantity_value_entry,
                                  rate_quantity_unit_entry,
                                  drug_form_administration_unit_code_entry,
                                  dosage_form_entry,
                                  dose_quantity_extra_entry,
                                  package_size_entry,
                                  active_ingredient_raw_entry,
                                  match_description)
                select distinct dp.id,
                                dp.id_original_drug,
                                dp.epi_id,
                                dp.epi_type,
                                dp.effective_time,
                                dp.drug_code,
                                dp.active_ingredient,
                                dp.active_ingredient_est,
                                dp.active_ingredient_latin,
                                dp.active_ingredient_eng,
                                dp.drug_name,
                                dp.dose_quantity_value,
                                dp.dose_quantity_unit,
                                dp.rate_quantity_value,
                                dp.rate_quantity_unit,
                                dp.drug_form_administration_unit_code_display_name,
                                dp.recipe_code,
                                dp.package_size,
                                dp.text_type,
                                dp.active_ingredient_raw,
                
                                de.id,
                                de.epi_id,
                                de.epi_type,
                                de.drug_id,
                                de.prescription_type,
                                de.effective_time,
                                de.drug_code,
                                de.active_ingredient,
                                de.active_ingredient_est,
                                de.active_ingredient_latin,
                                de.active_ingredient_eng,
                                de.dose_quantity_value,
                                de.dose_quantity_unit,
                                de.rate_quantity_value,
                                de.rate_quantity_unit,
                                de.drug_form_administration_unit_code,
                                de.drug_form_administration_unit_code_display_name,
                                de.dose_quantity_extra,
                                de.package_size,
                                de.active_ingredient_raw,
                                'epi_id, drug_code, active_ingredient,  dose_quantity_value, dose_quantity_unit'
                from {source_entry} as de
                         join {source_parsed} as dp
                              on
                                      de.epi_id = dp.epi_id and
                                      de.drug_code = dp.drug_code and
                                      de.dose_quantity_value = dp.dose_quantity_value and
                                      de.dose_quantity_unit = dp.dose_quantity_unit and
                                      (de.active_ingredient like '%' || dp.active_ingredient || '%')
                where
                  -- only interested in unresolved rows
                    (dp.id in (select id from {un_parsed}) or de.id in (select id from {un_entry}))
                  and
                  -- time is not contradicting 1. either both are NULL 2. one is NULL 3. both are equal
                    (de.effective_time is NULL or dp.effective_time is NULL or de.effective_time = dp.effective_time);        
            """
            ).format(
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step4Match_E_DC_ING_PT(CDAMatchingSubtask):
    """
    Match based on  epi_id, drug_code, active_ingredient, package_type.
    Insert them to a new table '*_drug_matched'. Decrease unresolved.
    """

    luigi_target_name = "Step4Match_E_DC_ING_PT_done"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 4")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} (id_parsed,
                                  id_original_drug_parsed,
                                  epi_id_parsed,
                                  epi_type_parsed,
                                  effective_time_parsed,
                                  drug_code_parsed,
                                  active_ingredient_parsed,
                                  active_ingredient_est_parsed,
                                  active_ingredient_latin_parsed,
                                  active_ingredient_eng_parsed,
                                  drug_name_parsed,
                                  dose_quantity_value_parsed,
                                  dose_quantity_unit_parsed,
                                  rate_quantity_value_parsed,
                                  rate_quantity_unit_parsed,
                                  dosage_form_parsed,
                                  recipe_code_parsed,
                                  package_size_parsed,
                                  text_type_parsed,
                                  active_ingredient_raw_parsed,
                                  id_entry,
                                  epi_id_entry,
                                  epi_type_entry,
                                  drug_id_entry,
                                  prescription_type_entry,
                                  effective_time_entry,
                                  drug_code_entry,
                                  active_ingredient_entry,
                                  active_ingredient_est_entry,
                                  active_ingredient_latin_entry,
                                  active_ingredient_eng_entry,
                                  dose_quantity_value_entry,
                                  dose_quantity_unit_entry,
                                  rate_quantity_value_entry,
                                  rate_quantity_unit_entry,
                                  drug_form_administration_unit_code_entry,
                                  dosage_form_entry,
                                  dose_quantity_extra_entry,
                                  package_size_entry,
                                  active_ingredient_raw_entry,
                                  match_description)
            select distinct dp.id,
                            dp.id_original_drug,
                            dp.epi_id,
                            dp.epi_type,
                            dp.effective_time,
                            dp.drug_code,
                            dp.active_ingredient,
                            dp.active_ingredient_est,
                            dp.active_ingredient_latin,
                            dp.active_ingredient_eng,
                            dp.drug_name,
                            dp.dose_quantity_value,
                            dp.dose_quantity_unit,
                            dp.rate_quantity_value,
                            dp.rate_quantity_unit,
                            dp.drug_form_administration_unit_code_display_name,
                            dp.recipe_code,
                            dp.package_size,
                            dp.text_type,
                            dp.active_ingredient_raw,
            
                            de.id,
                            de.epi_id,
                            de.epi_type,
                            de.drug_id,
                            de.prescription_type,
                            de.effective_time,
                            de.drug_code,
                            de.active_ingredient,
                            de.active_ingredient_est,
                            de.active_ingredient_latin,
                            de.active_ingredient_eng,
                            de.dose_quantity_value,
                            de.dose_quantity_unit,
                            de.rate_quantity_value,
                            de.rate_quantity_unit,
                            de.drug_form_administration_unit_code,
                            de.drug_form_administration_unit_code_display_name,
                            de.dose_quantity_extra,
                            de.package_size,
                            de.active_ingredient_raw,
                            'epi_id, drug_code, active_ingredient, drug_form_administration_unit_code_display_name '
            from {source_entry} as de
                     join {source_parsed} as dp
                          on
                                  de.epi_id = dp.epi_id and
                                  de.drug_code = dp.drug_code and
                                  de.active_ingredient like '%' || dp.active_ingredient || '%' and
                                  -- matches for example parsed: silmatilgad, entry: silmatilgad, lahus
                                  de.drug_form_administration_unit_code_display_name like
                                  '%' || dp.drug_form_administration_unit_code_display_name || '%'
            where
              -- only interested in unresolved rows
                (dp.id in (select id from {un_parsed}) or de.id in (select id from {un_entry}))
              and
              -- time is not contradicting 1. either both are NULL 2. one is NULL 3. both are equal
                (de.effective_time is NULL or dp.effective_time is NULL or de.effective_time = dp.effective_time);
            """
            ).format(
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step5Match_E_DC_ING(CDAMatchingSubtask):
    """
    Match based on  epi_id, drug_code, active_ingredient.
    Insert them to a new table '*_drug_matched'. Decrease unresolved.
    """

    luigi_target_name = "Step5Match_E_DC_ING_done"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 5")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} (id_parsed,
                                  id_original_drug_parsed,
                                  epi_id_parsed,
                                  epi_type_parsed,
                                  effective_time_parsed,
                                  drug_code_parsed,
                                  active_ingredient_parsed,
                                  active_ingredient_est_parsed,
                                  active_ingredient_latin_parsed,
                                  active_ingredient_eng_parsed,
                                  drug_name_parsed,
                                  dose_quantity_value_parsed,
                                  dose_quantity_unit_parsed,
                                  rate_quantity_value_parsed,
                                  rate_quantity_unit_parsed,
                                  dosage_form_parsed,
                                  recipe_code_parsed,
                                  package_size_parsed,
                                  text_type_parsed,
                                  active_ingredient_raw_parsed,
                                  id_entry,
                                  epi_id_entry,
                                  epi_type_entry,
                                  drug_id_entry,
                                  prescription_type_entry,
                                  effective_time_entry,
                                  drug_code_entry,
                                  active_ingredient_entry,
                                  active_ingredient_est_entry,
                                  active_ingredient_latin_entry,
                                  active_ingredient_eng_entry,
                                  dose_quantity_value_entry,
                                  dose_quantity_unit_entry,
                                  rate_quantity_value_entry,
                                  rate_quantity_unit_entry,
                                  drug_form_administration_unit_code_entry,
                                  dosage_form_entry,
                                  dose_quantity_extra_entry,
                                  package_size_entry,
                                  active_ingredient_raw_entry,
                                  match_description)
            select distinct dp.id,
                            dp.id_original_drug,
                            dp.epi_id,
                            dp.epi_type,
                            dp.effective_time,
                            dp.drug_code,
                            dp.active_ingredient,
                            dp.active_ingredient_est,
                            dp.active_ingredient_latin,
                            dp.active_ingredient_eng,
                            dp.drug_name,
                            dp.dose_quantity_value,
                            dp.dose_quantity_unit,
                            dp.rate_quantity_value,
                            dp.rate_quantity_unit,
                            dp.drug_form_administration_unit_code_display_name,
                            dp.recipe_code,
                            dp.package_size,
                            dp.text_type,
                            dp.active_ingredient_raw,
            
                            de.id,
                            de.epi_id,
                            de.epi_type,
                            de.drug_id,
                            de.prescription_type,
                            de.effective_time,
                            de.drug_code,
                            de.active_ingredient,
                            de.active_ingredient_est,
                            de.active_ingredient_latin,
                            de.active_ingredient_eng,
                            de.dose_quantity_value,
                            de.dose_quantity_unit,
                            de.rate_quantity_value,
                            de.rate_quantity_unit,
                            de.drug_form_administration_unit_code,
                            de.drug_form_administration_unit_code_display_name,
                            de.dose_quantity_extra,
                            de.package_size,
                            de.active_ingredient_raw,
                            'epi_id, drug_code, active_ingredient'
            from {source_entry} as de
                     join {source_parsed} as dp
                          on
                                  de.epi_id = dp.epi_id and
                                  de.drug_code = dp.drug_code and
                                  (de.active_ingredient like '%' || dp.active_ingredient || '%')
            where
              -- only interested in unresolved rows
                (dp.id in (select id from {un_parsed}) or de.id in (select id from {un_entry}))
              and
              -- time is not contradicting 1. either both are NULL 2. one is NULL 3. both are equal
                (de.effective_time is NULL or dp.effective_time is NULL or de.effective_time = dp.effective_time);
        
            """
            ).format(
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step6Match_E_ING_PT(CDAMatchingSubtask):
    """
    Match based on  epi_id, active_ingredient, package_type
    Insert them to a new table '*_drug_matched'. Decrease unresolved.
    """

    luigi_target_name = "Step6Match_E_ING_PT"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 6")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} (id_parsed,
                      id_original_drug_parsed,
                      epi_id_parsed,
                      epi_type_parsed,
                      effective_time_parsed,
                      drug_code_parsed,
                      active_ingredient_parsed,
                      active_ingredient_est_parsed,
                      active_ingredient_latin_parsed,
                      active_ingredient_eng_parsed,
                      drug_name_parsed,
                      dose_quantity_value_parsed,
                      dose_quantity_unit_parsed,
                      rate_quantity_value_parsed,
                      rate_quantity_unit_parsed,
                      dosage_form_parsed,
                      recipe_code_parsed,
                      package_size_parsed,
                      text_type_parsed,
                      active_ingredient_raw_parsed,
                      id_entry,
                      epi_id_entry,
                      epi_type_entry,
                      drug_id_entry,
                      prescription_type_entry,
                      effective_time_entry,
                      drug_code_entry,
                      active_ingredient_entry,
                      active_ingredient_est_entry,
                      active_ingredient_latin_entry,
                      active_ingredient_eng_entry,
                      dose_quantity_value_entry,
                      dose_quantity_unit_entry,
                      rate_quantity_value_entry,
                      rate_quantity_unit_entry,
                      drug_form_administration_unit_code_entry,
                      dosage_form_entry,
                      dose_quantity_extra_entry,
                      package_size_entry,
                      active_ingredient_raw_entry,
                      match_description)
            select distinct dp.id,
                            dp.id_original_drug,
                            dp.epi_id,
                            dp.epi_type,
                            dp.effective_time,
                            dp.drug_code,
                            dp.active_ingredient,
                            dp.active_ingredient_est,
                            dp.active_ingredient_latin,
                            dp.active_ingredient_eng,
                            dp.drug_name,
                            dp.dose_quantity_value,
                            dp.dose_quantity_unit,
                            dp.rate_quantity_value,
                            dp.rate_quantity_unit,
                            dp.drug_form_administration_unit_code_display_name,
                            dp.recipe_code,
                            dp.package_size,
                            dp.text_type,
                            dp.active_ingredient_raw,
            
                            de.id,
                            de.epi_id,
                            de.epi_type,
                            de.drug_id,
                            de.prescription_type,
                            de.effective_time,
                            de.drug_code,
                            de.active_ingredient,
                            de.active_ingredient_est,
                            de.active_ingredient_latin,
                            de.active_ingredient_eng,
                            de.dose_quantity_value,
                            de.dose_quantity_unit,
                            de.rate_quantity_value,
                            de.rate_quantity_unit,
                            de.drug_form_administration_unit_code,
                            de.drug_form_administration_unit_code_display_name,
                            de.dose_quantity_extra,
                            de.package_size,
                            de.active_ingredient_raw,
                            'epi_id, active_ingredient, drug_form_administration_unit_code_display_name'
            from {source_entry} as de
                     join {source_parsed} as dp
                          on
                                  de.epi_id = dp.epi_id and
                                  (de.active_ingredient like '%' || dp.active_ingredient || '%') and
                                  de.drug_form_administration_unit_code_display_name like
                                  '%' || dp.drug_form_administration_unit_code_display_name || '%'
            where
              -- only interested in unresolved rows
                (dp.id in (select id from {un_parsed}) or de.id in (select id from {un_entry}))
              and
              -- drug_codes must not be contradicting, it is okay when: 1. both are NULL 2. only one of them is null.
              -- The case where drug_codes equal is covered in step 4)
                (de.drug_code is null or dp.drug_code is null)
              and
              -- time is not contradicting 1. either both are NULL 2. one is NULL 3. both are equal
                (de.effective_time is NULL or dp.effective_time is NULL or de.effective_time = dp.effective_time);
        """
            ).format(
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step7Match_E_ING(CDAMatchingSubtask):
    """
    Match based on  epi_id, active_ingredient.
    Insert them to a new table '*_drug_matched'. Decrease unresolved.
    """

    luigi_target_name = "Step7Match_E_ING_done"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 7")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} (id_parsed,
                                  id_original_drug_parsed,
                                  epi_id_parsed,
                                  epi_type_parsed,
                                  effective_time_parsed,
                                  drug_code_parsed,
                                  active_ingredient_parsed,
                                  active_ingredient_est_parsed,
                                  active_ingredient_latin_parsed,
                                  active_ingredient_eng_parsed,
                                  drug_name_parsed,
                                  dose_quantity_value_parsed,
                                  dose_quantity_unit_parsed,
                                  rate_quantity_value_parsed,
                                  rate_quantity_unit_parsed,
                                  dosage_form_parsed,
                                  recipe_code_parsed,
                                  package_size_parsed,
                                  text_type_parsed,
                                  active_ingredient_raw_parsed,
                                  id_entry,
                                  epi_id_entry,
                                  epi_type_entry,
                                  drug_id_entry,
                                  prescription_type_entry,
                                  effective_time_entry,
                                  drug_code_entry,
                                  active_ingredient_entry,
                                  active_ingredient_est_entry,
                                  active_ingredient_latin_entry,
                                  active_ingredient_eng_entry,
                                  dose_quantity_value_entry,
                                  dose_quantity_unit_entry,
                                  rate_quantity_value_entry,
                                  rate_quantity_unit_entry,
                                  drug_form_administration_unit_code_entry,
                                  dosage_form_entry,
                                  dose_quantity_extra_entry,
                                  package_size_entry,
                                  active_ingredient_raw_entry,
                                  match_description)
            select distinct dp.id,
                            dp.id_original_drug,
                            dp.epi_id,
                            dp.epi_type,
                            dp.effective_time,
                            dp.drug_code,
                            dp.active_ingredient,
                            dp.active_ingredient_est,
                            dp.active_ingredient_latin,
                            dp.active_ingredient_eng,
                            dp.drug_name,
                            dp.dose_quantity_value,
                            dp.dose_quantity_unit,
                            dp.rate_quantity_value,
                            dp.rate_quantity_unit,
                            dp.drug_form_administration_unit_code_display_name,
                            dp.recipe_code,
                            dp.package_size,
                            dp.text_type,
                            dp.active_ingredient_raw,
            
                            de.id,
                            de.epi_id,
                            de.epi_type,
                            de.drug_id,
                            de.prescription_type,
                            de.effective_time,
                            de.drug_code,
                            de.active_ingredient,
                            de.active_ingredient_est,
                            de.active_ingredient_latin,
                            de.active_ingredient_eng,
                            de.dose_quantity_value,
                            de.dose_quantity_unit,
                            de.rate_quantity_value,
                            de.rate_quantity_unit,
                            de.drug_form_administration_unit_code,
                            de.drug_form_administration_unit_code_display_name,
                            de.dose_quantity_extra,
                            de.package_size,
                            de.active_ingredient_raw,
                            'epi_id, active_ingredient'
            from {source_entry} as de
                     join {source_parsed} as dp
                          on
                                  de.epi_id = dp.epi_id and
                                  (de.active_ingredient like '%' || dp.active_ingredient || '%')
            where
              -- only interested in unresolved rows
                (dp.id in (select id from {un_parsed}) or de.id in (select id from {un_entry}))
              and
              -- package types are not contradicting
              -- either of the package types is NULL, the case where package_types are equal is covered in step 6
                (
                        de.drug_form_administration_unit_code_display_name is NULL or
                        dp.drug_form_administration_unit_code_display_name is NULL
                    )
              and
              -- drug_codes must not be contradicting
              -- it is okay when: 1. both are NULL 2. only one of them is null
              -- the case where drug_codes are equal is covered in step 4
                (de.drug_code is null or dp.drug_code is null)
              and
              -- time is not contradicting 1. either both are NULL 2. one is NULL 3. both are equal
                (de.effective_time is NULL or dp.effective_time is NULL or de.effective_time = dp.effective_time);
            """
            ).format(
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step8Match_E_DC(CDAMatchingSubtask):
    """
    Match based on  epi_id, drug_code.
    Insert them to a new table '*_drug_matched'. Decrease unresolved.
    """

    luigi_target_name = "Step8Match_E_DC_done"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 8")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} (id_parsed,
                                  id_original_drug_parsed,
                                  epi_id_parsed,
                                  epi_type_parsed,
                                  effective_time_parsed,
                                  drug_code_parsed,
                                  active_ingredient_parsed,
                                  active_ingredient_est_parsed,
                                  active_ingredient_latin_parsed,
                                  active_ingredient_eng_parsed,
                                  drug_name_parsed,
                                  dose_quantity_value_parsed,
                                  dose_quantity_unit_parsed,
                                  rate_quantity_value_parsed,
                                  rate_quantity_unit_parsed,
                                  dosage_form_parsed,
                                  recipe_code_parsed,
                                  package_size_parsed,
                                  text_type_parsed,
                                  active_ingredient_raw_parsed,
                                  id_entry,
                                  epi_id_entry,
                                  epi_type_entry,
                                  drug_id_entry,
                                  prescription_type_entry,
                                  effective_time_entry,
                                  drug_code_entry,
                                  active_ingredient_entry,
                                  active_ingredient_est_entry,
                                  active_ingredient_latin_entry,
                                  active_ingredient_eng_entry,
                                  dose_quantity_value_entry,
                                  dose_quantity_unit_entry,
                                  rate_quantity_value_entry,
                                  rate_quantity_unit_entry,
                                  drug_form_administration_unit_code_entry,
                                  dosage_form_entry,
                                  dose_quantity_extra_entry,
                                  package_size_entry,
                                  active_ingredient_raw_entry,
                                  match_description)
            select distinct dp.id,
                            dp.id_original_drug,
                            dp.epi_id,
                            dp.epi_type,
                            dp.effective_time,
                            dp.drug_code,
                            dp.active_ingredient,
                            dp.active_ingredient_est,
                            dp.active_ingredient_latin,
                            dp.active_ingredient_eng,
                            dp.drug_name,
                            dp.dose_quantity_value,
                            dp.dose_quantity_unit,
                            dp.rate_quantity_value,
                            dp.rate_quantity_unit,
                            dp.drug_form_administration_unit_code_display_name,
                            dp.recipe_code,
                            dp.package_size,
                            dp.text_type,
                            dp.active_ingredient_raw,
            
                            de.id,
                            de.epi_id,
                            de.epi_type,
                            de.drug_id,
                            de.prescription_type,
                            de.effective_time,
                            de.drug_code,
                            de.active_ingredient,
                            de.active_ingredient_est,
                            de.active_ingredient_latin,
                            de.active_ingredient_eng,
                            de.dose_quantity_value,
                            de.dose_quantity_unit,
                            de.rate_quantity_value,
                            de.rate_quantity_unit,
                            de.drug_form_administration_unit_code,
                            de.drug_form_administration_unit_code_display_name,
                            de.dose_quantity_extra,
                            de.package_size,
                            de.active_ingredient_raw,
                            'epi_id, drug_code'
            from {source_entry} as de
                     join {source_parsed} as dp
                          on
                                  de.epi_id = dp.epi_id and
                                  de.drug_code = dp.drug_code
            where
              -- only interested in unresolved rows
                (dp.id in (select id from {un_parsed}) or de.id in (select id from {un_entry}))
              and
              -- package types are not contradicting
              -- either of the package types is NULL, the case where drug_codes are equal is covered in step 6
                (
                        de.drug_form_administration_unit_code_display_name is NULL or
                        dp.drug_form_administration_unit_code_display_name is NULL
                    )
              and
              -- time is not contradicting 1. either both are NULL 2. one is NULL 3. both are equal
                (de.effective_time is NULL or dp.effective_time is NULL or de.effective_time = dp.effective_time);            
            """
            ).format(
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step9AddUnique2(CDAMatchingSubtask):
    """
    From all the remaining unresolved rows, insert to '*_drug_matched' epicrisis that only exist in
    either parsed or entry (unique rows). Decrease unresolved.
    """

    luigi_target_name = "Step9AddUnique2_done"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 9")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            -- Parsed unique
            insert into {target} (id_parsed,
                                  id_original_drug_parsed,
                                  epi_id_parsed,
                                  epi_type_parsed,
                                  effective_time_parsed,
                                  drug_code_parsed,
                                  active_ingredient_parsed,
                                  active_ingredient_est_parsed,
                                  active_ingredient_latin_parsed,
                                  active_ingredient_eng_parsed,
                                  drug_name_parsed,
                                  dose_quantity_value_parsed,
                                  dose_quantity_unit_parsed,
                                  rate_quantity_value_parsed,
                                  rate_quantity_unit_parsed,
                                  dosage_form_parsed,
                                  recipe_code_parsed,
                                  package_size_parsed,
                                  text_type_parsed,
                                  active_ingredient_raw_parsed,
                                  match_description)
            with entry_unresolved_epis as
                     (
                         -- only interested in unresolved entry rows
                         select de.epi_id
                         from {source_entry} as de
                         where de.id in (select id from {un_entry})
                     )
                 -- if epi exists only in parsed, then it is unique for parsed and no need/possibility to match it
            select dp.id,
                   dp.id_original_drug,
                   dp.epi_id,
                   dp.epi_type,
                   dp.effective_time,
                   dp.drug_code,
                   dp.active_ingredient,
                   dp.active_ingredient_est,
                   dp.active_ingredient_latin,
                   dp.active_ingredient_eng,
                   dp.drug_name,
                   dp.dose_quantity_value,
                   dp.dose_quantity_unit,
                   dp.rate_quantity_value,
                   dp.rate_quantity_unit,
                   dp.drug_form_administration_unit_code_display_name,
                   dp.recipe_code,
                   dp.package_size,
                   dp.text_type,
                   dp.active_ingredient_raw,
                   'unique_parsed'
            from {source_parsed} dp
            where dp.epi_id not in (select epi_id from entry_unresolved_epis)
              and dp.id in (select id from {un_parsed});

            
            -- Entry unique            
            insert into {target} (id_entry,
                                  epi_id_entry,
                                  epi_type_entry,
                                  drug_id_entry,
                                  prescription_type_entry,
                                  effective_time_entry,
                                  drug_code_entry,
                                  active_ingredient_entry,
                                  active_ingredient_est_entry,
                                  active_ingredient_latin_entry,
                                  active_ingredient_eng_entry,
                                  dose_quantity_value_entry,
                                  dose_quantity_unit_entry,
                                  rate_quantity_value_entry,
                                  rate_quantity_unit_entry,
                                  drug_form_administration_unit_code_entry,
                                  dosage_form_entry,
                                  dose_quantity_extra_entry,
                                  package_size_entry,
                                  active_ingredient_raw_entry,
                                  match_description)
            with parsed_unresolved_epis as
                     (
                         -- only interested in unresolved parsed rows
                         select de.epi_id
                         from {source_parsed} as de
                         where de.id in (select id from {un_parsed})
                     )
                 -- if epi exists only in entry, then it is unique for entry and no need/possibility to match it
            select de.id,
                   de.epi_id,
                   de.epi_type,
                   de.drug_id,
                   de.prescription_type,
                   de.effective_time,
                   de.drug_code,
                   de.active_ingredient,
                   de.active_ingredient_est,
                   de.active_ingredient_latin,
                   de.active_ingredient_eng,
                   de.dose_quantity_value,
                   de.dose_quantity_unit,
                   de.rate_quantity_value,
                   de.rate_quantity_unit,
                   de.drug_form_administration_unit_code,
                   de.drug_form_administration_unit_code_display_name,
                   de.dose_quantity_extra,
                   de.package_size,
                   de.active_ingredient_raw,
                   'unique_entry'
            from {source_entry} de
            where de.epi_id not in (select epi_id from parsed_unresolved_epis)
              and de.id in (select id from {un_entry});
            """
            ).format(
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step10AddUnresolved(CDAMatchingSubtask):
    """
    Add the remaining unresolved rows to '*_drug_matched'.
    """

    luigi_target_name = "Step10AddUnresolved_done"
    source_tables = ["source_entry", "source_text", "unresolved_entry_rows", "unresolved_text_rows"]
    altered_tables = ["matched"]

    def run(self):
        self.log_current_action("Matching 10")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} (id_parsed,
                                  id_original_drug_parsed,
                                  epi_id_parsed,
                                  epi_type_parsed,
                                  effective_time_parsed,
                                  drug_code_parsed,
                                  active_ingredient_parsed,
                                  active_ingredient_est_parsed,
                                  active_ingredient_latin_parsed,
                                  active_ingredient_eng_parsed,
                                  drug_name_parsed,
                                  dose_quantity_value_parsed,
                                  dose_quantity_unit_parsed,
                                  rate_quantity_value_parsed,
                                  rate_quantity_unit_parsed,
                                  dosage_form_parsed,
                                  recipe_code_parsed,
                                  package_size_parsed,
                                  text_type_parsed,
                                  active_ingredient_raw_parsed,
                                  match_description)
            select dp.id,
                   dp.id_original_drug,
                   dp.epi_id,
                   dp.epi_type,
                   dp.effective_time,
                   dp.drug_code,
                   dp.active_ingredient,
                   dp.active_ingredient_est,
                   dp.active_ingredient_latin,
                   dp.active_ingredient_eng,
                   dp.drug_name,
                   dp.dose_quantity_value,
                   dp.dose_quantity_unit,
                   dp.rate_quantity_value,
                   dp.rate_quantity_unit,
                   dp.drug_form_administration_unit_code_display_name,
                   dp.recipe_code,
                   dp.package_size,
                   dp.text_type,
                   dp.active_ingredient_raw,
                   'unresolved_parsed'
            from {source_parsed} as dp
            where id in (select id from {un_parsed});
                        
            insert into {target} (id_entry,
                                  epi_id_entry,
                                  epi_type_entry,
                                  drug_id_entry,
                                  prescription_type_entry,
                                  effective_time_entry,
                                  drug_code_entry,
                                  active_ingredient_entry,
                                  active_ingredient_est_entry,
                                  active_ingredient_latin_entry,
                                  active_ingredient_eng_entry,
                                  dose_quantity_value_entry,
                                  dose_quantity_unit_entry,
                                  rate_quantity_value_entry,
                                  rate_quantity_unit_entry,
                                  drug_form_administration_unit_code_entry,
                                  dosage_form_entry,
                                  dose_quantity_extra_entry,
                                  package_size_entry,
                                  active_ingredient_raw_entry,
                                  match_description)
            select de.id,
                   de.epi_id,
                   de.epi_type,
                   de.drug_id,
                   de.prescription_type,
                   de.effective_time,
                   de.drug_code,
                   de.active_ingredient,
                   de.active_ingredient_est,
                   de.active_ingredient_latin,
                   de.active_ingredient_eng,
                   de.dose_quantity_value,
                   de.dose_quantity_unit,
                   de.rate_quantity_value,
                   de.rate_quantity_unit,
                   de.drug_form_administration_unit_code,
                   de.drug_form_administration_unit_code_display_name,
                   de.dose_quantity_extra,
                   de.package_size,
                   de.active_ingredient_raw,
                   'unresolved_entry'
            from {source_entry} de
            where id in (select id from {un_entry});
 
            drop table {un_entry};
            drop table {un_parsed};
            """
            ).format(
                target=self.tables.matched,
                source_entry=self.tables.source_entry,
                source_parsed=self.tables.source_text,
                un_entry=self.tables.unresolved_entry_rows,
                un_parsed=self.tables.unresolved_text_rows,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class Step11DeleteDuplicates(CDAMatchingSubtask):
    """
    Deletes duplicates produced during matching.
    """

    luigi_target_name = "Step11DeleteDuplicates_done"
    source_tables = ["matched"]
    target_tables = ["matched_wo_duplicates"]

    def run(self):
        self.log_current_action("Matching 11")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};
            
            drop table if exists {target_wo_duplicates};
            create table {target_wo_duplicates} as           
            select *
            from (
                     select *,
                            row_number() over (partition by
                                (
                                 epi_id_entry,
                                 epi_type_entry,
                                 drug_id_entry,
                                 prescription_type_entry,
                                 effective_time_entry,
                                 dose_quantity_value_entry,
                                 dose_quantity_unit_entry,
                                 rate_quantity_value_entry,
                                 rate_quantity_unit_entry,
                                 drug_form_administration_unit_code_entry,
                                 dosage_form_entry,
                                 drug_code_entry,
                                 active_ingredient_entry,
                                 active_ingredient_est_entry,
                                 active_ingredient_latin_entry,
                                 active_ingredient_eng_entry,
                                 package_size_entry,
                                 epi_id_parsed,
                                 epi_type_parsed,
                                 effective_time_parsed,
                                 dose_quantity_value_parsed,
                                 dose_quantity_unit_parsed,
                                 rate_quantity_value_parsed,
                                 rate_quantity_unit_parsed,
                                 dosage_form_parsed,
                                 drug_code_parsed,
                                 active_ingredient_parsed,
                                 active_ingredient_est_parsed,
                                 active_ingredient_latin_parsed,
                                 active_ingredient_eng_parsed,
                                 drug_name_parsed,
                                 recipe_code_parsed,
                                 package_size_parsed,
                                 text_type_parsed,
                                 match_description
                                    )
                                order by epi_id_parsed desc) rn
                     from {target}
                 ) as tmp
            where rn = 1;
        
            reset role;
            """
            ).format(
                role=sql.Identifier(self.role),
                target=self.tables.matched,
                target_wo_duplicates=self.tables.matched_wo_duplicates,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
