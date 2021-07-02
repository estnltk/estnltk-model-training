import luigi
from psycopg2 import sql

from typing import Union, TYPE_CHECKING
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDABatchTask


class CreateFinalTable(CDABatchTask):
    """
    Creates final table from <prefix>_analysis_matched by choosing values from either entry or HTML column.

    Final table has columns:
        epi_id
        loinc_code
        elabor_t_lyhend
        analysis_name
        parameter_name
        parameter_unit
        time_series_block
        effective_time
        value
        value_type
        reference_values
        id_entry
        id_html
        match_description

        -- original uncleaned fields
        analysis_substrate_raw
        code_system_raw_entry
        code_system_name_raw_entry
        analysis_code_raw_entry
        parameter_code_raw_entry
        analysis_id_entry
        analysis_name_raw
        parameter_name_raw
        parameter_unit_raw
        effective_time_raw
        value_raw
        reference_values_raw
        substrate_html
        substrate_entry
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="analysis_matched")
    target_table = luigi.Parameter(default="analysis_cleaned")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        """
        Create the final table for resolved analysis data and add all possible matches into it.

        Although all sub-steps except for the the first can be executed in any order, we fix a particular order.
        This guarantees that the resulting table is always the same when the previous matching step is the same.
        """

        task_01 = CreateTable(parent_task=self, requirement=self.requirement)

        task_02 = AddUnresolvedEntry(parent_task=self, requirement=task_01)
        task_03 = AddUnresolvedHtml(parent_task=self, requirement=task_02)

        task_04 = AddVED(parent_task=self, requirement=task_03)
        task_05 = AddTies(parent_task=self, requirement=task_04)
        task_06 = AddEPV(parent_task=self, requirement=task_05)
        task_07 = AddEPD(parent_task=self, requirement=task_06)
        task_08 = AddEPPUDV(parent_task=self, requirement=task_07)

        task_09 = AddPerfectMatches(parent_task=self, requirement=task_08)
        task_10 = AddOnlyEntry(parent_task=self, requirement=task_09)
        task_11 = AddOnlyHtml(parent_task=self, requirement=task_10)

        task_12 = AddValueTypeColumn(parent_task=self, requirement=task_11)

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


class CDAFinalTableSubtask(CDASubtask):
    """
    A separate class for keeping the table names consistent throughout the entire finalisation step.

    The number of different temporal and non-temporal tables is to big to handle them as luigi parameters.
    Instead, the class uses a simple mechanical rules to derive them from the parameters of a parent class.

    Each subclass should be decorated with metainfo about its database usage and modification patterns.
    """

    # This must be here or luigi goes crazy
    parent_task = luigi.TaskParameter()

    class DatabaseTables:
        __slots__ = ["analysis_matched", "analysis_cleaned"]

        # needed because otherwise mypy complains
        #   - error: "DatabaseTables" has no attribute "analysis_matched"
        #   - error: "DatabaseTables" has no attribute "analysis_cleaned"
        if TYPE_CHECKING:
            analysis_matched = None  # type: str
            analysis_cleaned = None  # type: str

    def __init__(self, parent_task: CreateFinalTable, requirement: Union[luigi.TaskParameter, "CDAFinalTableSubtask"]):
        super().__init__(parent_task=parent_task, requirement=requirement)

        # Propagate parameters from the parent task
        self.config_file = parent_task.config_file
        self.role = parent_task.role
        self.schema = parent_task.schema
        self.luigi_targets_folder = parent_task.luigi_targets_folder

        self.tables = self.DatabaseTables()

        # Permanent tables
        self.tables.analysis_matched = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.source_table)
        )
        self.tables.analysis_cleaned = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.target_table)
        )

    @staticmethod
    def choose_between_html_and_entry(column_name, is_effective_time=False):
        """
        Chooses weather to keep value from *_html column or *_entry column.
        If
            * entry is not empty and html is empty -> keep entry
            * html is not empty and entry is empty -> keep html
            * both are null -> stays null
            * both are not empty but same -> keep html (does not matter which one to keep)
            * both are not empty but different -> keep html (actually not always the best choice!!!)
        Note that if field contains <ANONYM ...> tag, then it is considered to be NULL.
        There is a seperate clause for effective time, because the column type is timestamp and not varchar.
        """
        if is_effective_time:
            return sql.SQL(
                """
                case
                   when {col_html} is not null and {col_entry} is null then {col_html}
                   when {col_entry} is not null and {col_html} is null then {col_entry}
                   else {col_html}
                end as {col}
                """
            ).format(
                col_html=sql.Identifier(column_name + "_html"),
                col_entry=sql.Identifier(column_name + "_entry"),
                col=sql.Identifier(column_name),
            )
        else:
            return sql.SQL(
                """
                -- ::text makes sure comaprison '~' is made between text objects
                case
                    when {col_html} is not null and ({col_entry} is null or {col_entry}::text ~ 'ANONYM') 
                        then {col_html}
                    when {col_entry} is not null and ({col_html} is null or {col_html}::text ~ 'ANONYM') 
                        then {col_entry}
                    else {col_html}
                    end as {col}
                """
            ).format(
                col_html=sql.Identifier(column_name + "_html"),
                col_entry=sql.Identifier(column_name + "_entry"),
                col=sql.Identifier(column_name),
            )


class CreateTable(CDAFinalTableSubtask):
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("Creating final table")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {target};
            create table {target}
            (
                epi_id                   varchar,
                loinc_code               varchar,
                elabor_t_lyhend          varchar,
                analysis_name            varchar,
                parameter_name           varchar,
                parameter_unit           varchar,
                time_series_block        integer,
                effective_time           timestamp without time zone,
                value                    varchar,
                value_type               varchar,
                reference_values         varchar,
                --epi_type                 varchar,
                id_entry                 integer,
                id_html                  integer,
                value_type_entry         varchar,
                value_type_html          varchar,
                match_description        varchar,

                -- original uncleaned fields      
                analysis_substrate_raw    varchar,
                code_system_raw_entry     varchar,
                code_system_name_raw_entry varchar,
                analysis_code_raw_entry  varchar,
                parameter_code_raw_entry varchar,
                analysis_id_entry        integer,
                analysis_name_raw        varchar,
                parameter_name_raw       varchar,
                parameter_unit_raw       varchar,
                effective_time_raw       varchar,
                value_raw                varchar,
                reference_values_raw     varchar,
                substrate_html           varchar,
                substrate_entry          varchar
            );
            """
            ).format(role=sql.Identifier(self.role), target=self.tables.analysis_cleaned)
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddOnlyHtml(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddOnlyHtml")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_html               as epi_id,
                loinc_code_html           as loinc_code,
                elabor_t_lyhend_html      as elabor_t_lyhend,
                analysis_name_html        as analysis_name,
                parameter_name_html       as parameter_name,
                parameter_unit_html       as parameter_unit,
                time_series_block_html    as time_series_block,
                effective_time_html       as effective_time,
                value_html                as value,
                NULL                      as value_type, -- calculated in the end
                reference_values_html     as reference_values,
                --epi_type_html             as epi_type,
                id_entry                  as id_entry,
                id_html                   as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html   as analysis_substrate_raw, --exists only in HTML
                code_system_raw_entry,      -- exist only in ENTRY
                code_system_name_raw_entry, -- exist only in ENTRY
                analysis_code_raw_entry,  -- exist only in ENTRY
                parameter_code_raw_entry, -- exist only in ENTRY
                analysis_id_entry,        -- exist only in ENTRY
                analysis_name_raw_html    as analysis_name_raw,
                parameter_name_raw_html   as parameter_name_raw,
                parameter_unit_raw_html   as parameter_unit_raw,
                effective_time_raw_html   as effective_time_raw,
                value_raw_html            as value_raw,
                reference_values_raw_html as reference_values_raw,
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 'only_html'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(source=self.tables.analysis_matched, target=self.tables.analysis_cleaned)
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddOnlyEntry(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddOnlyEntry")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_entry               as epi_id,
                loinc_code_entry           as loinc_code,
                elabor_t_lyhend_entry      as elabor_t_lyhend,
                analysis_name_entry        as analysis_name,
                parameter_name_entry       as parameter_name,
                parameter_unit_entry       as parameter_unit,
                time_series_block_entry    as time_series_block,
                effective_time_entry       as effective_time,
                value_entry                as value,
                NULL                      as value_type, -- calculated in the end
                reference_values_entry     as reference_values,
                --epi_type_entry             as epi_type,
                id_entry                   as id_entry,
                id_html                    as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html   as analysis_substrate_raw, --exists only in HTML
                code_system_raw_entry,        -- exist only in ENTRY
                code_system_name_raw_entry,   -- exist only in ENTRY
                analysis_code_raw_entry,  -- exist only in ENTRY
                parameter_code_raw_entry, -- exist only in ENTRY
                analysis_id_entry,        -- exist only in ENTRY
                analysis_name_raw_entry    as analysis_name_raw,
                parameter_name_raw_entry   as parameter_name_raw,
                parameter_unit_raw_entry   as parameter_unit_raw,
                effective_time_raw_entry   as effective_time_raw,
                value_raw_entry            as value_raw,
                reference_values_raw_entry as reference_values_raw,               
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 'only_entry'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(source=self.tables.analysis_matched, target=self.tables.analysis_cleaned)
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddPerfectMatches(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddPerfectMatches")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_html             as epi_id,
                loinc_code_html           as loinc_code,
                    elabor_t_lyhend_html      as elabor_t_lyhend,
                    analysis_name_html      as analysis_name,
                    parameter_name_html     as parameter_name,
                    parameter_unit_html     as parameter_unit,
                    time_series_block_html  as time_series_block,
                    effective_time_html     as effective_time,
                    value_html              as value,
                    NULL                    as value_type,         --calculated in the end
                    reference_values_html   as reference_values,
                    --epi_type_html           as epi_type,
                    id_entry                  as id_entry,
                    id_html                   as id_html,
                    value_type_entry,
                    value_type_html,
                    match_description,
                    analysis_substrate_raw_html   as analysis_substrate_raw, --exists only in HTML
                    code_system_raw_entry,    -- exist only in ENTRY
                    code_system_name_raw_entry,  -- exist only in ENTRY
                    analysis_code_raw_entry,  -- exist only in ENTRY
                    parameter_code_raw_entry, -- exist only in ENTRY
                    analysis_id_entry,        -- exist only in ENTRY
                    analysis_name_raw_html    as analysis_name_raw,
                    parameter_name_raw_html   as parameter_name_raw,
                    parameter_unit_raw_html   as parameter_unit_raw,
                    effective_time_raw_html   as effective_time_raw,
                    value_raw_html            as value_raw,
                    reference_values_raw_html as reference_values_raw,
                    substrate_html,
                    substrate_entry
                from {source}
                where match_description = 
                    'analysis_name, parameter_name, parameter_unit, effective_time, value, reference_values'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(source=self.tables.analysis_matched, target=self.tables.analysis_cleaned)
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddEPPUDV(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddEPPUDV")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_html             as epi_id,
                loinc_code_html         as loinc_code,
                elabor_t_lyhend_html    as elabor_t_lyhend,
                {analysis_name_query},
                parameter_name_html     as parameter_name,     --same in both entry and HTML
                parameter_unit_html     as parameter_unit,     --same in both entry and HTML
                {time_series_block_query},
                effective_time_html     as effective_time,     --same in both entry and HTML
                value_html              as value,              --same in both entry and HTML
                NULL                    as value_type,         --calculated in the end
                {reference_values_query},
                --epi_type_html           as epi_type,
                id_entry                as id_entry,
                id_html                 as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html as analysis_substrate_raw, --exists only in HTML
                code_system_raw_entry,                         -- exist only in ENTRY
                code_system_name_raw_entry,                   -- exist only in ENTRY
                analysis_code_raw_entry,                       -- exist only in ENTRY
                parameter_code_raw_entry,                      -- exist only in ENTRY
                analysis_id_entry,                             -- exist only in ENTRY
                {analysis_name_raw_query},    
                parameter_name_raw_html as parameter_name_raw,
                parameter_unit_raw_html as parameter_unit_raw,
                effective_time_raw_html as effective_time_raw,
                value_raw_html          as value_raw,
                {reference_values_raw_query},
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 'epi_id, parameter_name, parameter_unit, effective_time, value'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(
                source=self.tables.analysis_matched,
                target=self.tables.analysis_cleaned,
                analysis_name_query=self.choose_between_html_and_entry("analysis_name"),
                reference_values_query=self.choose_between_html_and_entry("reference_values"),
                analysis_name_raw_query=self.choose_between_html_and_entry("analysis_name_raw"),
                reference_values_raw_query=self.choose_between_html_and_entry("reference_values_raw"),
                time_series_block_query=self.choose_between_html_and_entry("time_series_block"),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddEPD(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddEPD")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_html             as epi_id,             --same in both
                loinc_code_html         as loinc_code,
                elabor_t_lyhend_html    as elabor_t_lyhend,
                {analysis_name_query},
                parameter_name_html     as parameter_name,     --same in both
                {parameter_unit_query},
                {time_series_block_query},
                effective_time_html     as effective_time,     --same in both
                NULL                    as value,              --same in both
                NULL                    as value_type,         --calculated in the end
                {reference_values_query},
                --epi_type_html             as epi_type,
                id_entry                as id_entry,
                id_html                 as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html as analysis_substrate_raw, --exists only in HTML
                code_system_raw_entry,                         -- exist only in ENTRY
                code_system_name_raw_entry,                    -- exist only in ENTRY
                analysis_code_raw_entry,                       -- exist only in ENTRY
                parameter_code_raw_entry,                      -- exist only in ENTRY
                analysis_id_entry,                             -- exist only in ENTRY
                {analysis_name_raw_query},
                parameter_name_raw_html as parameter_name_raw,
                {parameter_unit_raw_query},
                effective_time_raw_html as effective_time_raw,
                NULL                    as value_raw,
                {reference_values_raw_query},
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 
                'epi_id, parameter_name, effective_time, value_html = NULL and value_entry = NULL'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;;   
            """
            ).format(
                source=self.tables.analysis_matched,
                target=self.tables.analysis_cleaned,
                analysis_name_query=self.choose_between_html_and_entry("analysis_name"),
                reference_values_query=self.choose_between_html_and_entry("reference_values"),
                parameter_unit_query=self.choose_between_html_and_entry("parameter_unit"),
                analysis_name_raw_query=self.choose_between_html_and_entry("analysis_name_raw"),
                reference_values_raw_query=self.choose_between_html_and_entry("reference_values_raw"),
                parameter_unit_raw_query=self.choose_between_html_and_entry("parameter_unit_raw"),
                time_series_block_query=self.choose_between_html_and_entry("time_series_block"),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddEPV(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddEPV")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_html             as epi_id,         --same in both                                        
                    loinc_code_html         as loinc_code,
                    elabor_t_lyhend_html    as elabor_t_lyhend,
                    {analysis_name_query},        
                    parameter_name_html     as parameter_name, --same in both
                    {parameter_unit_query},
                    {time_series_block_query},
                    {effective_time_query},
                    value_html              as value,          --same in both
                    NULL                      as value_type,   --calculated in the end
                    {reference_values_query},
                    --epi_type_html             as epi_type,
                    id_entry                as id_entry,
                    id_html                 as id_html,
                    value_type_entry,
                    value_type_html,                
                    match_description,
                    analysis_substrate_raw_html   as analysis_substrate_raw, --exists only in HTML
                    code_system_raw_entry,                     -- exist only in ENTRY
                    code_system_name_raw_entry,                -- exist only in ENTRY
                    analysis_code_raw_entry,                   -- exist only in ENTRY
                    parameter_code_raw_entry,                  -- exist only in ENTRY
                    analysis_id_entry,    
                    {analysis_name_raw_query},
                    parameter_name_raw_html as parameter_name_raw,
                    {parameter_unit_raw_query},
                    {effective_time_raw_query},
                    value_raw_html          as value_raw,      --same in both
                    {reference_values_raw_query},
                    substrate_html,
                    substrate_entry
                from {source}
                where match_description = 
                    'epi_id, parameter_name, value, effective_time_html = NULL or effective_time_entry = NULL'    
                order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(
                source=self.tables.analysis_matched,
                target=self.tables.analysis_cleaned,
                analysis_name_query=self.choose_between_html_and_entry("analysis_name"),
                reference_values_query=self.choose_between_html_and_entry("reference_values"),
                parameter_unit_query=self.choose_between_html_and_entry("parameter_unit"),
                effective_time_query=self.choose_between_html_and_entry("effective_time", is_effective_time=True),
                analysis_name_raw_query=self.choose_between_html_and_entry("analysis_name_raw"),
                reference_values_raw_query=self.choose_between_html_and_entry("reference_values_raw"),
                parameter_unit_raw_query=self.choose_between_html_and_entry("parameter_unit_raw"),
                effective_time_raw_query=self.choose_between_html_and_entry(
                    "effective_time_raw", is_effective_time=True
                ),
                time_series_block_query=self.choose_between_html_and_entry("time_series_block"),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddTies(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddTies")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_html               as epi_id,
                loinc_code_html           as loinc_code,
                elabor_t_lyhend_html      as elabor_t_lyhend,
                analysis_name_html        as analysis_name,
                parameter_name_html       as parameter_name,
                parameter_unit_html       as parameter_unit,
                time_series_block_html    as time_series_block,
                effective_time_html       as effective_time,
                value_html                as value,
                NULL                      as value_type, -- calculated in the end
                reference_values_html     as reference_values,
                --epi_type_html             as epi_type,
                id_entry                  as id_entry,
                id_html                   as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html   as analysis_substrate_raw, --exists only in HTML
                code_system_raw_entry,                         -- exist only in ENTRY
                code_system_name_raw_entry,                    -- exist only in ENTRY
                analysis_code_raw_entry,                       -- exist only in ENTRY
                parameter_code_raw_entry,                      -- exist only in ENTRY
                analysis_id_entry,                             -- exist only in ENTRY
                analysis_name_raw_html    as analysis_name_raw,
                parameter_name_raw_html   as parameter_name_raw,
                parameter_unit_raw_html   as parameter_unit_raw,
                effective_time_raw_html   as effective_time_raw,
                value_raw_html            as value_raw,
                reference_values_raw_html as reference_values_raw,
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 'html ties, parameter_name is not cleaned correctly'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(
                source=self.tables.analysis_matched, target=self.tables.analysis_cleaned,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_entry              as epi_id,
                loinc_code_html           as loinc_code,
                elabor_t_lyhend_html      as elabor_t_lyhend,
                analysis_name_entry       as analysis_name,
                parameter_name_entry      as parameter_name,
                parameter_unit_entry      as parameter_unit,
                time_series_block_entry,
                effective_time_entry      as effective_time,
                value_entry               as value,
                NULL                      as value_type, -- calculated in the end
                reference_values_entry    as reference_values,
                --epi_type_entry            as epi_type,
                id_entry                  as id_entry,
                id_html                   as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html   as analysis_substrate_raw, --exists only in HTML
                code_system_raw_entry,    -- exist only in ENTRY
                code_system_name_raw_entry, -- exist only in ENTRY
                analysis_code_raw_entry,  -- exist only in ENTRY
                parameter_code_raw_entry, -- exist only in ENTRY
                analysis_id_entry,        -- exist only in ENTRY
                analysis_name_raw_html    as analysis_name_raw,
                parameter_name_raw_html   as parameter_name_raw,
                parameter_unit_raw_html   as parameter_unit_raw,
                effective_time_raw_html   as effective_time_raw,
                value_raw_html            as value_raw,
                reference_values_raw_html as reference_values_raw,
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 'entry ties, parameter_name is not cleaned correctly'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(source=self.tables.analysis_matched, target=self.tables.analysis_cleaned)
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddVED(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddVED")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_html             as epi_id,         --same in both
                loinc_code_html         as loinc_code,
                elabor_t_lyhend_html    as elabor_t_lyhend,
                {analysis_name_query},
                {parameter_name_query},
                {parameter_unit_query},
                {time_series_block_query},
                effective_time_html     AS effective_time, --same in both
                value_html              AS value,          --same in both
                NULL                      as value_type,   -- calculated in the end
                {reference_values_query},
                --epi_type_html            as epi_type,
                id_entry                as id_entry,
                id_html                 as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html   as analysis_substrate, --exists only in HTML
                code_system_raw_entry,                     -- exist only in ENTRY
                code_system_name_raw_entry,                -- exist only in ENTRY
                analysis_code_raw_entry,                   -- exist only in ENTRY
                parameter_code_raw_entry,                  -- exist only in ENTRY
                analysis_id_entry,                         -- exist only in ENTRY
                {analysis_name_raw_query},
                {parameter_name_raw_query},
                {parameter_unit_raw_query},
                effective_time_raw_html  as effective_time_raw,
                value_raw_html          as value_raw,      --same in both
                {reference_values_raw_query},
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 'epi_id, effective_time, value'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(
                source=self.tables.analysis_matched,
                target=self.tables.analysis_cleaned,
                analysis_name_query=self.choose_between_html_and_entry("analysis_name"),
                reference_values_query=self.choose_between_html_and_entry("reference_values"),
                parameter_name_query=self.choose_between_html_and_entry("parameter_name"),
                parameter_unit_query=self.choose_between_html_and_entry("parameter_unit"),
                analysis_name_raw_query=self.choose_between_html_and_entry("analysis_name_raw"),
                reference_values_raw_query=self.choose_between_html_and_entry("reference_values_raw"),
                parameter_name_raw_query=self.choose_between_html_and_entry("parameter_name_raw"),
                parameter_unit_raw_query=self.choose_between_html_and_entry("parameter_unit_raw"),
                time_series_block_query=self.choose_between_html_and_entry("time_series_block"),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddUnresolvedHtml(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddUnresolvedHtml")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_html               as epi_id, 
                loinc_code_html           as loinc_code,
                elabor_t_lyhend_html      as elabor_t_lyhend,
                analysis_name_html        as analysis_name,
                parameter_name_html       as parameter_name,
                parameter_unit_html       as parameter_unit,
                time_series_block_html as time_series_block,
                effective_time_html       as effective_time,
                value_html                as value,
                NULL                      as value_type, -- calculated in the end
                reference_values_html     as reference_values,
                --epi_type_html             as epi_type,
                id_entry                  as id_entry,
                id_html                   as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html   as analysis_substrate_raw, --exists only in HTML
                code_system_raw_entry,                         -- exist only in ENTRY
                code_system_name_raw_entry,                    -- exist only in ENTRY
                analysis_code_raw_entry,                       -- exist only in ENTRY
                parameter_code_raw_entry,                      -- exist only in ENTRY
                analysis_id_entry,                             -- exist only in ENTRY
                analysis_name_raw_html    as analysis_name_raw,
                parameter_name_raw_html   as parameter_name_raw,
                parameter_unit_raw_html   as parameter_unit_raw,
                effective_time_raw_html   as effective_time_raw,
                value_raw_html            as value_raw,
                reference_values_raw_html as reference_values_raw,
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 'unresolved_html'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(source=self.tables.analysis_matched, target=self.tables.analysis_cleaned)
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddUnresolvedEntry(CDAFinalTableSubtask):
    source_tables = ["analysis_matched"]
    target_tables = ["analysis_cleaned"]

    def run(self):
        self.log_current_action("AddUnresolvedEntry")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {target} 
            select 
                epi_id_entry              as epi_id,
                loinc_code_entry          as loinc_code,
                elabor_t_lyhend_entry     as elabor_t_lyhend,
                analysis_name_entry       as analysis_name,
                parameter_name_entry      as parameter_name,
                parameter_unit_entry      as parameter_unit,
                time_series_block_entry   as time_series_block,
                effective_time_entry      as effective_time,
                value_entry               as value,
                NULL                      as value_type, -- calculated in the end
                reference_values_entry    as reference_values,
                --epi_type_html             as epi_type,
                id_entry                  as id_entry,
                id_html                   as id_html,
                value_type_entry,
                value_type_html,
                match_description,
                analysis_substrate_raw_html   as analysis_substrate_raw, --exists only in HTML
                code_system_raw_entry,    -- exist only in ENTRY
                code_system_name_raw_entry,   -- exist only in ENTRY
                analysis_code_raw_entry,  -- exist only in ENTRY
                parameter_code_raw_entry, -- exist only in ENTRY
                analysis_id_entry,        -- exist only in ENTRY
                analysis_name_raw_html    as analysis_name_raw,
                parameter_name_raw_html   as parameter_name_raw,
                parameter_unit_raw_html   as parameter_unit_raw,
                effective_time_raw_html   as effective_time_raw,
                value_raw_html            as value_raw,
                reference_values_raw_html as reference_values_raw,
                substrate_html,
                substrate_entry
            from {source}
            where match_description = 'unresolved_entry'
            order by epi_id, effective_time,analysis_name, parameter_name, parameter_unit, value;
            """
            ).format(source=self.tables.analysis_matched, target=self.tables.analysis_cleaned)
        )
        conn.commit()
        cur.close()
        conn.close()
        self.mark_as_complete()


class AddValueTypeColumn(CDAFinalTableSubtask):
    """
    Based on columns 'value_type_html' and 'value_type_entry' insert values to 'value_type'
    Delete columns 'value_type_html' and 'value_type_entry'.
    """

    altered_tables = ["analysis_matched"]

    def run(self):
        self.log_current_action("AddValueTypeColumn")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """        
                set search_path to {schema};
                        
                update {target}
                set value_type =
                    case
                        -- check_types does not recognize cleaned value type "range"
                        when value_type_entry = value_type_html or value_type_entry = 'range' or value_type_html = 'range'
                            then coalesce(value_type_html, value_type_entry)
                        else check_types(value)
                    end;
                                 
                -- no need anymore for separate value types for entry and html
                alter table {target}
                drop column value_type_html;
                alter table {target}
                drop column value_type_entry;              
            """
            ).format(schema=sql.Identifier(self.schema), target=self.tables.analysis_cleaned)
        )
        conn.commit()
        cur.close()
        conn.close()
        self.mark_as_complete()
