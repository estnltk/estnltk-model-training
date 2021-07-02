import luigi
from psycopg2 import sql

from typing import Union, List, TYPE_CHECKING
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDABatchTask


class MatchEntryHtml(CDABatchTask):
    """
    A batch task that executes many substeps to match analysis data from two sources:
        - <prefix>_analysis_entry_loinced_unique
        - <prefix>_analysis_html_loinced_unique

    During the substeps html and entry table are being matched based on different column combinations.

    As a result two new tables are created:
        - <prefix>_analysis_matched
        - <prefix>_analysis_ties

    Matched
        - The matched table contains columns from both html and entry table that have been merged together as much as possible.
        - The source table of every column is indicated in the end of column names
            (e.g. analysis_name -> analysis_name_html or analysis_name_entry).
        - If there was no match found for a row, then the non-source columns will have NULL values
            (e.g. html table row did not have a match -> all columns ending with '_entry' will have NULL values)
        - Column 'match_description' indicates which column values were used to match  <prefix>_entry_loinced_unique and
            <prefix>_html_loinced_unique.

    Ties
        - during cleaning column parameter_name important information is lost which makes it impossible to match some rows
        - this table contains information about those rows

    Indices on tables
        - <prefix>_entry_loinced_unique
        - <prefix>_html_loinced_unique
    are needed before matching, therefore they are created before matching process starts.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    html_source_table = luigi.Parameter(default="analysis_html_loinced_unique")
    entry_source_table = luigi.Parameter(default="analysis_entry_loinced_unique")
    matched_table = luigi.Parameter(default="matched")
    ties_table = luigi.Parameter(default="ties")
    table_prefix = luigi.Parameter(default="")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def requires(self):
        self.log_current_action("Matching HTML and ENTRY sources")
        task_00 = CreateAnalysisMatchedTable(parent_task=self, requirement=self.requirement)
        task_01 = CreateUnresolvedTables(parent_task=self, requirement=task_00)
        task_02 = AddUniqueMatchesToMatchedTable(parent_task=self, requirement=task_01)
        task_03 = AddPerfectMatchesToMatchedTable(parent_task=self, requirement=task_02)
        task_04 = CreateMatchingVEDP(parent_task=self, requirement=task_03)
        task_05 = MatchVEDPU(parent_task=self, requirement=task_04)
        task_06 = AddMatchedVEDP(parent_task=self, requirement=task_05)
        task_07 = DetectTies(parent_task=self, requirement=task_06)
        task_08 = MatchEPD(parent_task=self, requirement=task_07)
        task_09 = MatchVEP(parent_task=self, requirement=task_08)
        task_10 = MatchVED(parent_task=self, requirement=task_09)
        task_11 = AddUnmatchedToMatched(parent_task=self, requirement=task_10)
        return [task_01, task_02, task_03, task_04, task_05, task_06, task_07, task_08, task_09, task_10, task_11]


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
            "source_html",
            "analysis_matched",
            "ties",
            "unresolved_html_rows",
            "unresolved_entry_rows",
            "vedp_matches",
            "vedpu_matches",
            "edp_matches",
            "vep_matches",
            "ved_matches",
        ]

        # needed because otherwise mypy complains
        #   - error: "DatabaseTables" has no attribute "analysis_matched" etc
        if TYPE_CHECKING:
            source_entry = None  # type: str
            source_html = None  # type: str
            analysis_matched = None  # type: str
            ties = None  # type: str
            unresolved_html_rows = None  # type: str
            unresolved_entry_rows = None  # type: str
            vedp_matches = None  # type: str
            vedpu_matches = None  # type: str
            edp_matches = None  # type: str
            vep_matches = None  # type: str
            ved_matches = None  # type: str

    def __init__(self, parent_task: MatchEntryHtml, requirement: Union[luigi.TaskParameter, "CDAMatchingSubtask"]):
        super().__init__(parent_task=parent_task, requirement=requirement)

        # Propagate parameters from the parent task
        self.config_file = parent_task.config_file
        self.role = parent_task.role
        self.schema = parent_task.schema
        self.html_source_table = parent_task.html_source_table
        self.entry_source_table = parent_task.entry_source_table
        self.table_prefix = str(parent_task.table_prefix)
        self.luigi_targets_folder = parent_task.luigi_targets_folder

        self.tables = self.DatabaseTables()

        # Permanent tables
        self.tables.source_entry = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.entry_source_table)
        )
        self.tables.source_html = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.html_source_table)
        )
        self.tables.analysis_matched = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.matched_table)
        )
        self.tables.ties = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(parent_task.ties_table)
        )

        # Temporary tables for unresolved rows
        self.tables.unresolved_html_rows = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "unresolved_html")
        )
        self.tables.unresolved_entry_rows = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "unresolved_entry")
        )

        # Temporary tables for finding various matches
        self.tables.vedp_matches = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "vedp_matches")
        )
        self.tables.vedpu_matches = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "vedpu_matches")
        )
        self.tables.edp_matches = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "edp_matches")
        )
        self.tables.vep_matches = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "vep_matches")
        )
        self.tables.ved_matches = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + self.table_prefix + "ved_matches")
        )

        # Test that meta-info about table usage is more or less consistent
        # TODO: Make this work
        # assert set(self.source_tables) <= set(self.tables.__slots__), 'Source tables description is invalid'
        # assert set(self.target_tables) <= set(self.tables.__slots__), 'Target tables description is invalid'
        # assert set(self.altered_tables) <= set(self.tables.__slots__), 'Altered tables description is invalid'
        # assert set(self.deleted_tables) <= set(self.tables.__slots__), 'Deleted tables description is invalid'

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
    
            delete from {unmatched_html}
            where id in (select id_html from {matched});
            """
            ).format(
                matched=self.tables.analysis_matched,
                unmatched_html=self.tables.unresolved_html_rows,
                unmatched_entry=self.tables.unresolved_entry_rows,
            )
        )
        cur.connection.commit()


class CreateAnalysisMatchedTable(CDAMatchingSubtask):
    """
    Create an empty table where later matched rows can be inserted.
    """

    source_tables: List[str] = []
    target_tables = ["analysis_matched"]

    def run(self):
        self.log_current_action("Matching 0")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};
            
            drop table if exists {matched};
            create table {matched} (
                id_html                          integer,
                id_entry                         bigint,
                row_nr_html                      integer,
                epi_id_html                      varchar,
                epi_type_html                    varchar,
                parse_type_html                  varchar,
                panel_id_html                    integer,
                analysis_name_raw_html           varchar,
                parameter_name_raw_html          varchar,
                parameter_unit_raw_html          varchar,
                reference_values_raw_html        varchar,
                value_raw_html                   varchar,
                analysis_substrate_raw_html      varchar,
                effective_time_raw_html          varchar,
                analysis_name_html               varchar,
                parameter_name_html              varchar,
                effective_time_html              timestamp,
                value_html                       varchar,
                parameter_unit_from_suffix_html  varchar,
                suffix_html                      varchar,
                value_type_html                  varchar,
                time_series_block_html           integer,
                reference_values_html            varchar,
                parameter_unit_html              varchar,
                loinc_unit_html                  varchar,
                substrate_html                   varchar,
                elabor_t_lyhend_html             varchar,
                loinc_code_html                  varchar,
                source_html                      varchar,
                original_analysis_entry_id       bigint,
                epi_id_entry                     varchar,
                epi_type_entry                   varchar,
                analysis_id_entry                bigint,
                code_system_raw_entry            varchar,
                code_system_name_raw_entry       varchar,
                analysis_code_raw_entry          varchar,
                analysis_name_raw_entry          varchar,
                parameter_code_raw_entry         varchar,
                parameter_name_raw_entry         varchar,
                parameter_unit_raw_entry         varchar,
                reference_values_raw_entry       varchar,
                effective_time_raw_entry         varchar,
                value_raw_entry                  varchar,
                analysis_name_entry              varchar,
                parameter_name_entry             varchar,
                effective_time_entry             timestamp,
                value_entry                      varchar,
                parameter_unit_from_suffix_entry varchar,
                suffix_entry                     varchar,
                value_type_entry                 varchar,
                time_series_block_entry          integer,
                reference_values_entry           varchar,
                parameter_unit_entry             varchar,
                loinc_unit_entry                 varchar,
                substrate_entry                  varchar,
                elabor_t_lyhend_entry            varchar,
                loinc_code_entry                 varchar,
                source_entry                     varchar,
                match_description                varchar
            );

            """
            ).format(role=sql.Identifier(self.role), matched=self.tables.analysis_matched)
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class CreateUnresolvedTables(CDAMatchingSubtask):
    """
    Creating two tables (for entry and html) containing id's of the rows that are still unresolved.
    Every time some rows are matched, their id-s are removed from unresolved tables.
    """

    source_tables = ["entry_source", "html_source"]
    target_tables = ["unresolved_entry_rows", "unresolved_html_rows"]

    def run(self):
        self.log_current_action("Matching 1")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            -- all HTML id's where epi_id exists BOTH in entry and HTML
            -- the rest (which are not in the table) are unique for HTML and therefore do not need resolving
            drop table if exists {unresolved_html_rows};
            create table {unresolved_html_rows} as
            select id
            from {source_html} as html
            where html.epi_id in (select epi_id from {source_entry});

            -- all ENTRY id's where epi_id exists BOTH in entry and HTML
            -- the rest (which are not in the table) are unique for HTML and therefore do not need resolving
            drop table if exists {unresolved_entry_rows};
            create table {unresolved_entry_rows} as
            select id
            from {source_entry} as entry
            where entry.epi_id in (select epi_id from {source_html});
            """
            ).format(
                role=sql.Identifier(self.role),
                source_html=self.tables.source_html,
                source_entry=self.tables.source_entry,
                unresolved_html_rows=self.tables.unresolved_html_rows,
                unresolved_entry_rows=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddUniqueMatchesToMatchedTable(CDAMatchingSubtask):
    """
    Epicrisis that exist only in html or only entry  (are unique).
    """

    source_tables = ["entry_source", "html_source", "unresolved_html_rows", "unresolved_entry_rows"]
    altered_tables = ["analysis_matched", "unresolved_html_rows", "unresolved_entry_rows"]

    def run(self):
        self.log_current_action("Matching 2")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {matched} 
            (
                row_nr_html,
                epi_id_html,
                epi_type_html,
                parse_type_html,
                panel_id_html,
                analysis_name_raw_html,
                parameter_name_raw_html,
                parameter_unit_raw_html,
                reference_values_raw_html,
                effective_time_raw_html,
                value_raw_html,
                analysis_substrate_raw_html,
                --analysis_substrate_html,
                --entry_match_id_html,
                --entry_html_match_desc_html,
                id_html,
                analysis_name_html,
                parameter_name_html,
                effective_time_html,
                value_html,
                parameter_unit_from_suffix_html,
                suffix_html,
                value_type_html,
                time_series_block_html,
                reference_values_html,
                source_html,
                parameter_unit_html,
                loinc_unit_html,
                substrate_html,
                elabor_t_lyhend_html,
                loinc_code_html,
                match_description
            )
            select html.*, 'only_html' as match_description
            from {source_html} as html
            left join {unmatched_html} as un_html
            on html.id = un_html.id
            where un_html.id is null;
            """
            ).format(
                matched=self.tables.analysis_matched,
                source_html=self.tables.source_html,
                unmatched_html=self.tables.unresolved_html_rows,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
            insert into {matched} 
            (
                id_entry,
                original_analysis_entry_id,
                epi_id_entry,
                epi_type_entry,
                analysis_id_entry,
                code_system_raw_entry,
                code_system_name_raw_entry,
                analysis_code_raw_entry,
                analysis_name_raw_entry,
                parameter_code_raw_entry,
                parameter_name_raw_entry,
                parameter_unit_raw_entry,
                reference_values_raw_entry,
                effective_time_raw_entry,
                value_raw_entry,
                analysis_name_entry,
                parameter_name_entry,
                effective_time_entry,
                value_entry,
                parameter_unit_from_suffix_entry,
                suffix_entry,
                value_type_entry,
                time_series_block_entry,
                reference_values_entry,
                source_entry,
                parameter_unit_entry,
                loinc_unit_entry,
                substrate_entry,
                elabor_t_lyhend_entry,
                loinc_code_entry,
                match_description
            )
            select entry.*, 'only_entry' as match_description
            from {source_entry} as entry
            left join {unmatched_entry} as un_entry
            on entry.id = un_entry.id
            where un_entry.id is null;
            """
            ).format(
                matched=self.tables.analysis_matched,
                source_entry=self.tables.source_entry,
                unmatched_entry=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        # no need to update unresolved, because unique epicrisis were NOT added to the unresolved tables in the first
        # place
        # unresolved table contains only rows which have potential of overlapping in html and entry

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddPerfectMatchesToMatchedTable(CDAMatchingSubtask):
    """
    Add indices to source tables.

    Insert to <prefix>_analysis_matched table PERFECT matches where E, A, P, PU, D, V, R are exactly the same.
    In more detail:
        - html.epi_id = entry.epi_id
        - html.analysis_name = entry.analysis_name
        - html.parameter_name = entry.parameter_name
        - html.parameter_unit = entry.parameter_unit
        - html.effective_time = entry.effective_time
        - html.value = entry.value
        - html.reference_values = entry.reference_values;
    """

    source_tables = ["entry_source", "html_source"]
    target_tables = ["analysis_matched"]
    altered_tables = ["entry_source", "html_source", "unresolved_html_rows", "unresolved_entry_rows"]

    def run(self):
        self.log_current_action("Matching 3")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Create necessary indices for efficiency
        html_idx_prefix = "idx_" + self.html_source_table + "_"
        entry_idx_prefix = "idx_" + self.entry_source_table + "_"
        cur.execute(
            sql.SQL(
                """
            set role {role};

            -- add md5 for unbounded columns, otherwise "index row size x exceeds maximum 2712 for index"
            -- https://github.com/doorkeeper-gem/doorkeeper/wiki/How-to-fix-PostgreSQL-error-on-index-row-size
            create index if not exists {index_html_primary}
            on {source_html}(epi_id, md5(parameter_name), md5(value), effective_time);
            
            create index if not exists {index_entry_primary}
            on {source_entry}(epi_id, md5(parameter_name), md5(value), effective_time);

            -- index on analysis_name
            create index if not exists {index_html_a} on {source_html}(md5(analysis_name));
            create index if not exists {index_entry_a} on {source_entry}(md5(analysis_name));

            --index on parameter_unit    
            create index if not exists {index_html_pu} on {source_html}(md5(parameter_unit));
            create index if not exists {index_entry_pu} on {source_entry}(md5(parameter_unit));

            -- index on reference_values
            create index if not exists {index_html_r} on {source_html}(md5(reference_values));
            create index if not exists {index_entry_r} on {source_entry}(md5(reference_values));
            """
            ).format(
                schema=sql.Identifier(self.schema),
                role=sql.Identifier(self.role),
                index_html_primary=sql.Identifier(html_idx_prefix + "epi_id_parameter_name_value_effective_time"),
                index_entry_primary=sql.Identifier(entry_idx_prefix + "epi_id_parameter_name_value_effective_time"),
                index_html_a=sql.Identifier(html_idx_prefix + "analysis_name"),
                index_entry_a=sql.Identifier(entry_idx_prefix + "analysis_name"),
                index_html_pu=sql.Identifier(html_idx_prefix + "parameter_unit"),
                index_entry_pu=sql.Identifier(entry_idx_prefix + "parameter_unit"),
                index_html_r=sql.Identifier(html_idx_prefix + "reference_values"),
                index_entry_r=sql.Identifier(entry_idx_prefix + "reference_values"),
                source_entry=self.tables.source_entry,
                source_html=self.tables.source_html,
            )
        )
        conn.commit()

        # Create matched table
        cur.execute(
            sql.SQL(
                """
            set role {role};

            insert into {matched}
            select 
                html.id                                                          as id_html,
                entry.id                                                         as id_entry,
                html.row_nr                                                      as row_nr_html,
                html.epi_id                                                      as epi_id_html,
                html.epi_type                                                    as epi_type_html,
                html.parse_type                                                  as parse_type_html,
                html.panel_id                                                    as panel_id_html,
                html.analysis_name_raw                                           as analysis_name_raw_html,
                html.parameter_name_raw                                          as parameter_name_raw_html,
                html.parameter_unit_raw                                          as parameter_unit_raw_html,
                html.reference_values_raw                                        as reference_values_raw_html,
                html.value_raw                                                   as value_raw_html,
                html.analysis_substrate_raw                                      as analysis_substrate_raw_html,
                html.effective_time_raw                                          as effective_time_raw_html,
                html.analysis_name                                               as analysis_name_html,
                html.parameter_name                                              as parameter_name_html,
                html.effective_time                                              as effective_time_html,
                html.value                                                       as value_html,
                html.parameter_unit_from_suffix                                  as parameter_unit_from_suffix_html,
                html.suffix                                                      as suffix_html,
                html.value_type                                                  as value_type_html,
                html.time_series_block                                           as time_series_block_html,
                html.reference_values                                            as reference_values_html,
                html.parameter_unit                                              as parameter_unit_html,
                html.loinc_unit                                                  as loinc_unit_html,
                html.substrate                                                   as substrate_html,
                html.elabor_t_lyhend                                             as elabor_t_lyhend_html,
                html.loinc_code                                                  as loinc_code_html,
                html.source                                                      as source_html,
                entry.original_analysis_entry_id,
                entry.epi_id                                                     as epi_id_entry,
                entry.epi_type                                                   as epi_type_entry,
                entry.analysis_id                                                as analysis_id_entry,
                entry.code_system_raw                                            as code_system_raw_entry,
                entry.code_system_name_raw                                       as code_system_name_raw_entry,
                entry.analysis_code_raw                                          as analysis_code_raw_entry,
                entry.analysis_name_raw                                          as analysis_name_raw_entry,
                entry.parameter_code_raw                                         as parameter_code_raw_entry,
                entry.parameter_name_raw                                         as parameter_name_raw_entry,
                entry.parameter_unit_raw                                         as parameter_unit_raw_entry,
                entry.reference_values_raw                                       as reference_values_raw_entry,
                entry.effective_time_raw                                         as effective_time_raw_entry,
                entry.value_raw                                                  as value_raw_entry,
                entry.analysis_name                                              as analysis_name_entry,
                entry.parameter_name                                             as parameter_name_entry,
                entry.effective_time                                             as effective_time_entry,
                entry.value                                                      as value_entry,
                entry.parameter_unit_from_suffix                                 as parameter_unit_from_suffix_entry,
                entry.suffix                                                     as suffix_entry,
                entry.value_type                                                 as value_type_entry,
                entry.time_series_block                                          as time_series_block_entry,
                entry.reference_values                                           as reference_values_entry,
                entry.parameter_unit                                             as parameter_unit_entry,
                entry.loinc_unit                                                 as loinc_unit_entry,
                entry.substrate                                                  as substrate_entry,
                entry.elabor_t_lyhend                                            as elabor_t_lyhend_entry,
                entry.loinc_code                                                 as loinc_code_entry,
                entry.source                                                     as source_entry,
                'analysis_name, parameter_name, parameter_unit, effective_time, value, reference_values' 
                    as match_description
            from {source_html} as html
            join {source_entry} as entry
            on 
                html.epi_id = entry.epi_id and
                html.analysis_name = entry.analysis_name and
                html.parameter_name = entry.parameter_name and
                html.parameter_unit = entry.parameter_unit and
                date(html.effective_time) = date(entry.effective_time) and
                html.value = entry.value and
                html.reference_values = entry.reference_values;
            """
            ).format(
                role=sql.Identifier(self.role),
                matched=self.tables.analysis_matched,
                source_html=self.tables.source_html,
                source_entry=self.tables.source_entry,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class CreateMatchingVEDP(CDAMatchingSubtask):
    """
    We will continue work with UNRESOLVED rows.
    Html and entry table are matched based on columns V E D P
    In more detail:
        - html.epi_id = entry.epi_id,
        - html.value = entry.value,
        - html.effective_time = entry.effective_time
        - html.paramter_name = entry.parameter_name
    """

    source_tables = ["entry_source", "html_source", "unresolved_html_rows", "unresolved_entry_rows"]
    target_tables = ["vedp_matches"]

    def run(self):
        self.log_current_action("Matching 4")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {vedp_matches};
            create table {vedp_matches} as
            select 
                html.id                          as id_html,
                entry.id                         as id_entry,
                html.row_nr                      as row_nr_html,
                html.epi_id                      as epi_id_html,
                html.epi_type                    as epi_type_html,
                html.parse_type                  as parse_type_html,
                html.panel_id                    as panel_id_html,
                html.analysis_name_raw           as analysis_name_raw_html,
                html.parameter_name_raw          as parameter_name_raw_html,
                html.parameter_unit_raw          as parameter_unit_raw_html,
                html.reference_values_raw        as reference_values_raw_html,
                html.effective_time_raw          as effective_time_raw_html,
                html.value_raw                   as value_raw_html,
                html.analysis_substrate_raw      as analysis_substrate_raw_html,
                --html.analysis_substrate          as analysis_substrate_html,
                --html.entry_match_id              as entry_match_id_html,
                --html.entry_html_match_desc       as entry_html_match_desc_html,
                html.analysis_name               as analysis_name_html,
                html.parameter_name              as parameter_name_html,
                html.effective_time              as effective_time_html,
                html.value                       as value_html,
                html.parameter_unit_from_suffix  as parameter_unit_from_suffix_html,
                html.suffix                      as suffix_html,
                html.value_type                  as value_type_html,
                html.time_series_block           as time_series_block_html,
                html.reference_values            as reference_values_html,
                html.parameter_unit              as parameter_unit_html,
                html.loinc_unit                  as loinc_unit_html,
                html.substrate                   as substrate_html,
                html.elabor_t_lyhend             as elabor_t_lyhend_html,
                html.loinc_code                  as loinc_code_html,
                html.source                      as source_html,
                entry.original_analysis_entry_id,
                entry.epi_id                     as epi_id_entry,
                entry.epi_type                   as epi_type_entry,
                entry.analysis_id                as analysis_id_entry,
                entry.code_system_raw            as code_system_raw_entry,
                entry.code_system_name_raw       as code_system_name_raw_entry,
                entry.analysis_code_raw          as analysis_code_raw_entry,
                entry.analysis_name_raw          as analysis_name_raw_entry,
                entry.parameter_code_raw         as parameter_code_raw_entry,
                entry.parameter_name_raw         as parameter_name_raw_entry,
                entry.parameter_unit_raw         as parameter_unit_raw_entry,
                entry.reference_values_raw       as reference_values_raw_entry,
                entry.effective_time_raw         as effective_time_raw_entry,
                entry.value_raw                  as value_raw_entry,
                entry.analysis_name              as analysis_name_entry,
                entry.parameter_name             as parameter_name_entry,
                entry.effective_time             as effective_time_entry,
                entry.value                      as value_entry,
                entry.parameter_unit_from_suffix as parameter_unit_from_suffix_entry,
                entry.suffix                     as suffix_entry,
                entry.value_type                 as value_type_entry,
                entry.time_series_block          as time_series_block_entry,
                entry.reference_values           as reference_values_entry,
                entry.parameter_unit             as parameter_unit_entry,
                entry.loinc_unit                 as loinc_unit_entry,
                entry.substrate                  as substrate_entry,
                entry.elabor_t_lyhend            as elabor_t_lyhend_entry,
                entry.loinc_code                 as loinc_code_entry,
                entry.source                     as source_entry
            from {source_html} as html
            join {source_entry} as entry
            on 
                html.epi_id = entry.epi_id and
                html.parameter_name = entry.parameter_name and
                date(html.effective_time) = date(entry.effective_time) and
                html.value = entry.value
            left join {un_html} un_html
            on un_html.id = html.id
            left join {un_entry} un_entry
            on un_entry.id = entry.id
            where un_html.id is not null or un_entry.id is not null;
            """
            ).format(
                role=sql.Identifier(self.role),
                source_html=self.tables.source_html,
                source_entry=self.tables.source_entry,
                vedp_matches=self.tables.vedp_matches,
                un_html=self.tables.unresolved_html_rows,
                un_entry=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class MatchVEDPU(CDAMatchingSubtask):
    """
    1. We will continue work with VEDPU table, trying to match the rows by unit
      In more detail:
        - html.epi_id = entry.epi_id,
        - html.value = entry.value,
        - html.effective_time = entry.effective_time
        - html.parameter_name = entry.parameter_name
        - html.parameter_unit = entry.parameter_unit

    2. Dealing with the multiplicities (1 entry ----> many html or 1 html -----> many entry)
        Adding to matched table ONLY 1 <----> 1 matches.
    """

    source_tables = ["entry_source", "html_source", "vedp_matches"]
    target_tables = ["vedpu_matches"]
    altered_tables = ["analysis_matched", "unresolved_html_rows", "unresolved_entry_rows"]

    def run(self):
        self.log_current_action("Matching 5")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {vedpu_matches};
            create table {vedpu_matches} as
            select * from {vedp_matches}
            where
                -- different cases for the units to match
                -- (so NULL = NULL would be TRUE)
                (parameter_unit_html is not distinct from parameter_unit_entry) or
                --one table does not have unit, the other does
                (parameter_unit_entry is NULL and parameter_unit_html is NOT NULL) or
                (parameter_unit_html is NULL and parameter_unit_entry is NOT NULL);
            """
            ).format(
                role=sql.Identifier(self.role),
                vedp_matches=self.tables.vedp_matches,
                vedpu_matches=self.tables.vedpu_matches,
                un_entry=self.tables.unresolved_entry_rows,
                un_html=self.tables.unresolved_html_rows,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
            with vedpu_multip_entry as 
            (
                -- 1 to many entry
                select id_entry
                from {vedpu_matches}
                group by id_entry
                having count(*) >= 2
            ),
            vedpu_multip_html as 
            (
                -- 1 to many html
                select id_html
                from {vedpu_matches}
                group by id_html
                having count(*) >= 2
            )
            insert into {matched}
            (
                select 
                    vedpu.*, 
                    'epi_id, parameter_name, parameter_unit, effective_time, value'
                from {vedpu_matches} as vedpu
                -- keep only 1--1 relationships
                -- html is not in multiple matches
                left join vedpu_multip_html as html_vedpu
                on vedpu.id_html = html_vedpu.id_html
                -- entry is not in multiple matches
                left join vedpu_multip_entry as entry_vedpu
                on vedpu.id_entry = entry_vedpu.id_entry
                where html_vedpu.id_html is null and entry_vedpu.id_entry is null);

            drop table {vedpu_matches};
            """
            ).format(matched=self.tables.analysis_matched, vedpu_matches=self.tables.vedpu_matches)
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddMatchedVEDP(CDAMatchingSubtask):
    """
    Matching based on columns V E D P.
    In more detail:
        - html.epi_id = entry.epi_id,
        - html.value = entry.value,
        - html.effective_time = entry.effective_time
        - html.parameter_name = entry.parameter_name
    """

    source_tables = ["entry_source", "html_source", "vedp_matches"]
    altered_tables = ["analysis_matched", "unresolved_html_rows", "unresolved_entry_rows"]

    def run(self):
        self.log_current_action("Matching 6")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            with vedp_multip_entry as 
            (
                -- 1 to many entry
                select id_entry
                from {vedp_matches}
                group by id_entry
                having count(*) >= 2
            ),
            vedp_multip_html as 
            (
                -- 1 to many html
                select id_html
                from {vedp_matches}
                group by id_html
                having count(*) >= 2
            )
            insert into {matched}
            select vedp.*, 'epi_id, parameter_name, effective_time, value'
            from {vedp_matches} as vedp
             -- only 1 to 1 relationships
            left join vedp_multip_html as html_vedp
            on vedp.id_html = html_vedp.id_html
            left join vedp_multip_entry as entry_vedp
            on vedp.id_entry = entry_vedp.id_entry
            -- match only unresolved rows
            left join {un_html} un_html
            on un_html.id = html_vedp.id_html
            left join {un_entry} un_entry
            on un_entry.id = entry_vedp.id_entry
            where 
                (un_html.id is not null or un_entry.id is not null) and -- match only unresolved rows
                (html_vedp.id_html is null and entry_vedp.id_entry is null); -- only 1 to 1 relationships
            """
            ).format(
                matched=self.tables.analysis_matched,
                vedp_matches=self.tables.vedp_matches,
                un_html=self.tables.unresolved_html_rows,
                un_entry=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class DetectTies(CDAMatchingSubtask):
    """
    Detecting TIES
    NB! THEY WILL BE LEFT UNRESOLVED until PN cleaning is changed!!!

    example: epi_id = '19289924' and value = '-0.4'; could be avoided if PN_raw is not cleaned too much

    html_entry
    a -- b
      \ /
      / \
    c -- d
    """

    source_tables = ["entry_source", "html_source", "vedp_matches"]
    target_tables = ["ties"]
    altered_tables = ["matched", "unresolved_html_rows", "unresolved_entry_rows"]

    def run(self):
        self.log_current_action("Matching 7")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {tie};
            create table {tie} as
            with html_set as 
            (
                select 
                    array_agg(distinct id_html) as id_html,
                    epi_id_html,
                    value_html,
                    parameter_name_html,
                    --parameter_unit_html,
                    effective_time_html,
                    count(*) as count_html
                from {vedp_matches}
                group by epi_id_html, value_html, parameter_name_html, effective_time_html
            ),
            entry_set as 
            (
                 select 
                    array_agg(distinct id_entry) as id_entry,
                    epi_id_entry,
                    value_entry,
                    parameter_name_entry,
                    --parameter_unit_entry,
                    effective_time_entry,
                    count(*) as count_entry
                 from {vedp_matches}
                 group by epi_id_entry, value_entry, parameter_name_entry, effective_time_entry
            )
            select *
            from entry_set as e, html_set as h
            where 
                e.count_entry = h.count_html and 
                e.count_entry > 1 and -- we don't want 1 on 1 relationships
                parameter_name_entry = parameter_name_html and 
                value_entry = value_html and 
                effective_time_entry = effective_time_html and 
                epi_id_entry = epi_id_html;
            
            drop table {vedp_matches};
            """
            ).format(role=sql.Identifier(self.role), tie=self.tables.ties, vedp_matches=self.tables.vedp_matches)
        )
        conn.commit()

        # Removes ties from unresolved rows
        cur.execute(
            sql.SQL(
                """
            delete from {un_entry}
            where id in (select unnest(id_entry) from {tie});

            delete from {un_html}
            where id in (select unnest(id_html) from {tie});
            """
            ).format(
                tie=self.tables.ties,
                un_html=self.tables.unresolved_html_rows,
                un_entry=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
            insert into {matched} 
            (
                row_nr_html,
                epi_id_html,
                epi_type_html,
                parse_type_html,
                panel_id_html,
                analysis_name_raw_html,
                parameter_name_raw_html,
                parameter_unit_raw_html,
                reference_values_raw_html,
                effective_time_raw_html,
                value_raw_html,
                analysis_substrate_raw_html,
                id_html,
                analysis_name_html,
                parameter_name_html,
                effective_time_html,
                value_html,
                parameter_unit_from_suffix_html,
                suffix_html,
                value_type_html,
                time_series_block_html,
                reference_values_html,
                source_html,
                parameter_unit_html,
                loinc_unit_html,
                substrate_html,
                elabor_t_lyhend_html,
                loinc_code_html,
                match_description
            )
            select loinc_unique.*, 'html ties, parameter_name is not cleaned correctly' as match_description
            from {source_html} as loinc_unique
            where loinc_unique.id in (select unnest(id_html) from {tie});
            """
            ).format(tie=self.tables.ties, source_html=self.tables.source_html, matched=self.tables.analysis_matched)
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
            insert into {matched} 
            (
                id_entry,
                original_analysis_entry_id,
                epi_id_entry,
                epi_type_entry,
                analysis_id_entry,
                code_system_raw_entry,
                code_system_name_raw_entry,
                analysis_code_raw_entry,
                analysis_name_raw_entry,
                parameter_code_raw_entry,
                parameter_name_raw_entry,
                parameter_unit_raw_entry,
                reference_values_raw_entry,
                effective_time_raw_entry,
                value_raw_entry,
                analysis_name_entry,
                parameter_name_entry,
                effective_time_entry,
                value_entry,
                parameter_unit_from_suffix_entry,
                suffix_entry,
                value_type_entry,
                time_series_block_entry,
                reference_values_entry,
                source_entry,
                parameter_unit_entry,
                loinc_unit_entry,
                substrate_entry,
                elabor_t_lyhend_entry,
                loinc_code_entry,
                match_description
            )
            select loinc_unique.*, 'entry ties, parameter_name is not cleaned correctly' as match_description
            from {source_entry} as loinc_unique
            where loinc_unique.id in (select unnest(id_entry) from {tie});
            """
            ).format(tie=self.tables.ties, source_entry=self.tables.source_entry, matched=self.tables.analysis_matched)
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class MatchEPD(CDAMatchingSubtask):
    """

    Matching based on columns E P D where values missing from both tables.
    In more detail:
        - html.epi_id = entry.epi_id,
        - html.effective_time = entry.effective_time
        - html.parameter_name = entry.parameter_name
        - value_html = null and value_entry = null

    Detecting mutliplicities,  adding to matched table ONLY 1 <----> 1 matches
        - example
            id_html, id_entry,
            4060022, 9939,
            4059982, 9939
    """

    source_tables = ["entry_source", "html_source"]
    target_tables = ["edp_matches"]
    altered_tables = ["analysis_matched", "unresolved_html_rows", "unresolved_entry_rows"]

    def run(self):
        self.log_current_action("Matching 8")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {edp_matches};
            create table  {edp_matches} as
            with epd_matched as 
            (
                select 
                    html.id                          as id_html,
                    entry.id                         as id_entry,
                    html.row_nr                      as row_nr_html,
                    html.epi_id                      as epi_id_html,
                    html.epi_type                    as epi_type_html,
                    html.parse_type                  as parse_type_html,
                    html.panel_id                    as panel_id_html,
                    html.analysis_name_raw           as analysis_name_raw_html,
                    html.parameter_name_raw          as parameter_name_raw_html,
                    html.parameter_unit_raw          as parameter_unit_raw_html,
                    html.reference_values_raw        as reference_values_raw_html,
                    html.effective_time_raw          as effective_time_raw_html,
                    html.value_raw                   as value_raw_html,
                    html.analysis_substrate_raw      as analysis_substrate_raw_html,
                    --html.analysis_substrate          as analysis_substrate_html,
                    --html.entry_match_id              as entry_match_id_html,
                    --html.entry_html_match_desc       as entry_html_match_desc_html,
                    html.analysis_name               as analysis_name_html,
                    html.parameter_name              as parameter_name_html,
                    html.effective_time              as effective_time_html,
                    html.value                       as value_html,
                    html.parameter_unit_from_suffix  as parameter_unit_from_suffix_html,
                    html.suffix                      as suffix_html,
                    html.value_type                  as value_type_html,
                    html.time_series_block           as time_series_block_html,
                    html.reference_values            as reference_values_html,
                    html.parameter_unit              as parameter_unit_html,
                    html.loinc_unit                  as loinc_unit_html,
                    html.substrate                   as substrate_html,
                    html.elabor_t_lyhend             as elabor_t_lyhend_html,
                    html.loinc_code                  as loinc_code_html,
                    html.source                      as source_html,
                    entry.original_analysis_entry_id,
                    entry.epi_id                     as epi_id_entry,
                    entry.epi_type                   as epi_type_entry,
                    entry.analysis_id                as analysis_id_entry,
                    entry.code_system_raw            as code_system_raw_entry,
                    entry.code_system_name_raw       as code_system_name_raw_entry,
                    entry.analysis_code_raw          as analysis_code_raw_entry,
                    entry.analysis_name_raw          as analysis_name_raw_entry,
                    entry.parameter_code_raw         as parameter_code_raw_entry,
                    entry.parameter_name_raw         as parameter_name_raw_entry,
                    entry.parameter_unit_raw         as parameter_unit_raw_entry,
                    entry.reference_values_raw       as reference_values_raw_entry,
                    entry.effective_time_raw         as effective_time_raw_entry,
                    entry.value_raw                  as value_raw_entry,
                    entry.analysis_name              as analysis_name_entry,
                    entry.parameter_name             as parameter_name_entry,
                    entry.effective_time             as effective_time_entry,
                    entry.value                      as value_entry,
                    entry.parameter_unit_from_suffix as parameter_unit_from_suffix_entry,
                    entry.suffix                     as suffix_entry,
                    entry.value_type                 as value_type_entry,
                    entry.time_series_block          as time_series_block_entry,
                    entry.reference_values           as reference_values_entry,
                    entry.parameter_unit             as parameter_unit_entry,
                    entry.loinc_unit                 as loinc_unit_entry,
                    entry.substrate                  as substrate_entry,
                    entry.elabor_t_lyhend            as elabor_t_lyhend_entry,
                    entry.loinc_code                 as loinc_code_entry,
                    entry.source                     as source_entry
                from {source_html} as html
                join {source_entry} as entry
                on 
                    html.epi_id = entry.epi_id and
                    html.parameter_name = entry.parameter_name and
                    date(html.effective_time) = date(entry.effective_time)
                -- only match unresolved rows
                left join {un_html} un_html
                on un_html.id = html.id
                left join {un_entry} un_entry
                on un_entry.id = entry.id
                where un_html.id is not null or un_entry.id is not null
            )
            select * from epd_matched
            where value_html is null and value_entry is null;

            -- Mutliplicities are not considered matched

            with edp_multip_entry as 
            (
                -- 1 html many entry
                select id_entry
                from {edp_matches}
                group by id_entry
                having count(*) >= 2
            ),
            edp_multip_html as 
            (
                -- 1 entry many HTML
                select id_html
                from {edp_matches}
                group by id_html
                having count(*) >= 2
            )
            --add 1--1 rows to matched
            insert into {matched}
            select 
                edp.*, 
                'epi_id, parameter_name, effective_time, value_html = NULL and value_entry = NULL' as match_description
            from {edp_matches} as edp
            -- only 1 to 1 relationships
            left join edp_multip_html as html_edp
                on edp.id_html = html_edp.id_html
            left join edp_multip_entry as entry_edp
                on edp.id_entry = entry_edp.id_entry
            where html_edp.id_html is null and entry_edp.id_entry is null;

            drop table {edp_matches};
            """
            ).format(
                role=sql.Identifier(self.role),
                matched=self.tables.analysis_matched,
                edp_matches=self.tables.edp_matches,
                source_html=self.tables.source_html,
                source_entry=self.tables.source_entry,
                un_html=self.tables.unresolved_html_rows,
                un_entry=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class MatchVEP(CDAMatchingSubtask):
    """
    Matching based on columns V E P where date is missing from either of tables.
    In more detail:
        - html.epi_id = entry.epi_id,
        - html.parameter_name = entry.parameter_name
        - html.value = entry.value
        - where effective_time_html is null or effective_time_entry is null;
    2. Detecting multiplicities
        example: html = 2962412, entry = 66119, 66127
    """

    source_tables = ["entry_source", "html_source"]
    target_tables = ["vep_matches"]
    altered_tables = ["analysis_matched", "unresolved_html_rows", "unresolved_entry_rows"]

    def run(self):
        self.log_current_action("Matching 9")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {matching_vep};
            create table {matching_vep} as
            with vep_matched as 
            (
                select 
                    html.id                          as id_html,
                    entry.id                         as id_entry,
                    html.row_nr                      as row_nr_html,
                    html.epi_id                      as epi_id_html,
                    html.epi_type                    as epi_type_html,
                    html.parse_type                  as parse_type_html,
                    html.panel_id                    as panel_id_html,
                    html.analysis_name_raw           as analysis_name_raw_html,
                    html.parameter_name_raw          as parameter_name_raw_html,
                    html.parameter_unit_raw          as parameter_unit_raw_html,
                    html.reference_values_raw        as reference_values_raw_html,
                    html.effective_time_raw          as effective_time_raw_html,
                    html.value_raw                   as value_raw_html,
                    html.analysis_substrate_raw      as analysis_substrate_raw_html,
                    --html.analysis_substrate          as analysis_substrate_html,
                    --html.entry_match_id              as entry_match_id_html,
                    --html.entry_html_match_desc       as entry_html_match_desc_html,
                    html.analysis_name               as analysis_name_html,
                    html.parameter_name              as parameter_name_html,
                    html.effective_time              as effective_time_html,
                    html.value                       as value_html,
                    html.parameter_unit_from_suffix  as parameter_unit_from_suffix_html,
                    html.suffix                      as suffix_html,
                    html.value_type                  as value_type_html,
                    html.time_series_block           as time_series_block_html,
                    html.reference_values            as reference_values_html,
                    html.parameter_unit              as parameter_unit_html,
                    html.loinc_unit                  as loinc_unit_html,
                    html.substrate                   as substrate_html,
                    html.elabor_t_lyhend             as elabor_t_lyhend_html,
                    html.loinc_code                  as loinc_code_html,
                    html.source                      as source_html,
                    entry.original_analysis_entry_id,
                    entry.epi_id                     as epi_id_entry,
                    entry.epi_type                   as epi_type_entry,
                    entry.analysis_id                as analysis_id_entry,
                    entry.code_system_raw            as code_system_raw_entry,
                    entry.code_system_name_raw       as code_system_name_raw_entry,
                    entry.analysis_code_raw          as analysis_code_raw_entry,
                    entry.analysis_name_raw          as analysis_name_raw_entry,
                    entry.parameter_code_raw         as parameter_code_raw_entry,
                    entry.parameter_name_raw         as parameter_name_raw_entry,
                    entry.parameter_unit_raw         as parameter_unit_raw_entry,
                    entry.reference_values_raw       as reference_values_raw_entry,
                    entry.effective_time_raw         as effective_time_raw_entry,
                    entry.value_raw                  as value_raw_entry,
                    entry.analysis_name              as analysis_name_entry,
                    entry.parameter_name             as parameter_name_entry,
                    entry.effective_time             as effective_time_entry,
                    entry.value                      as value_entry,
                    entry.parameter_unit_from_suffix as parameter_unit_from_suffix_entry,
                    entry.suffix                     as suffix_entry,
                    entry.value_type                 as value_type_entry,
                    entry.time_series_block          as time_series_block_entry,
                    entry.reference_values           as reference_values_entry,
                    entry.parameter_unit             as parameter_unit_entry,
                    entry.loinc_unit                 as loinc_unit_entry,
                    entry.substrate                  as substrate_entry,
                    entry.elabor_t_lyhend            as elabor_t_lyhend_entry,
                    entry.loinc_code                 as loinc_code_entry,
                    entry.source                     as source_entry
                from {source_html} as html
                join {source_entry} as entry
                on 
                    html.epi_id = entry.epi_id and
                    html.parameter_name = entry.parameter_name and
                    html.value = entry.value
                -- only match unresolved rows
                left join {un_html} un_html
                on un_html.id = html.id
                left join {un_entry} un_entry
                on un_entry.id = entry.id
                where un_html.id is not null or un_entry.id is not null
            )
            select * from vep_matched
            where effective_time_html is null or effective_time_entry is null;            

            -- Multiplicities are not considered as matched        
            with vep_multip_entry as 
            (
                -- 1 HTML many entry
                select id_entry
                from {matching_vep}
                group by id_entry
                having count(*) >= 2
            ),
            vep_multip_html as 
            (
                 -- 1 entry many HTML
                 select id_html
                 from {matching_vep}
                 group by id_html
                 having count(*) >= 2
            )
            -- 1 to 1 relationships added to matched table
            insert into {matched}
            select 
                vep.*, 
                'epi_id, parameter_name, value, effective_time_html = NULL or effective_time_entry = NULL'  
                    as match_description
            from {matching_vep} as vep
            left join vep_multip_html as html_vep
            on vep.id_html = html_vep.id_html
            left join vep_multip_entry as entry_vep
            on vep.id_entry = entry_vep.id_entry
            where html_vep.id_html is null and entry_vep.id_entry is null;

            drop table {matching_vep};  
            """
            ).format(
                role=sql.Identifier(self.role),
                matched=self.tables.analysis_matched,
                matching_vep=self.tables.vep_matches,
                source_html=self.tables.source_html,
                source_entry=self.tables.source_entry,
                un_html=self.tables.unresolved_html_rows,
                un_entry=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class MatchVED(CDAMatchingSubtask):
    """
    Matching based on columns V E D (parameter_name is wrong) where parameter_name is missing from either of tables
    (mainly cases where in HTML PN is for some reason under AN).
    In more detail:
        - html.epi_id = entry.epi_id,
        - html.parameter_name = entry.parameter_name
        - html.value = entry.value
        - html.effective_time = entry.effective_time
        - parameter_name_entry is null or parameter_name_html is null

    Detecting multiplicities
        - example:  one id_entry = 8756, two id_html = 4910093, 4910073
        - mainly parse_type = 2.2 (PN under AN)

    """

    source_tables = ["entry_source", "html_source"]
    target_tables = ["ved_matches"]
    altered_tables = ["analysis_matched", "unresolved_html_rows", "unresolved_entry_rows"]

    def run(self):
        self.log_current_action("Matching 10")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            set role {role};

            drop table if exists {ved_matches};
            create table {ved_matches} as
            with ved_matched as 
            (
                select 
                    html.id                          as id_html,
                    entry.id                         as id_entry,
                    html.row_nr                      as row_nr_html,
                    html.epi_id                      as epi_id_html,
                    html.epi_type                    as epi_type_html,
                    html.parse_type                  as parse_type_html,
                    html.panel_id                    as panel_id_html,
                    html.analysis_name_raw           as analysis_name_raw_html,
                    html.parameter_name_raw          as parameter_name_raw_html,
                    html.parameter_unit_raw          as parameter_unit_raw_html,
                    html.reference_values_raw        as reference_values_raw_html,
                    html.effective_time_raw          as effective_time_raw_html,
                    html.value_raw                   as value_raw_html,
                    html.analysis_substrate_raw      as analysis_substrate_raw_html,
                    --html.analysis_substrate          as analysis_substrate_html,
                    --html.entry_match_id              as entry_match_id_html,
                    --html.entry_html_match_desc       as entry_html_match_desc_html,
                    html.analysis_name               as analysis_name_html,
                    html.parameter_name              as parameter_name_html,
                    html.effective_time              as effective_time_html,
                    html.value                       as value_html,
                    html.parameter_unit_from_suffix  as parameter_unit_from_suffix_html,
                    html.suffix                      as suffix_html,
                    html.value_type                  as value_type_html,
                    html.time_series_block           as time_series_block_html,
                    html.reference_values            as reference_values_html,
                    html.parameter_unit              as parameter_unit_html,
                    html.loinc_unit                  as loinc_unit_html,
                    html.substrate                   as substrate_html,
                    html.elabor_t_lyhend             as elabor_t_lyhend_html,
                    html.loinc_code                  as loinc_code_html,
                    html.source                      as source_html,
                    entry.original_analysis_entry_id,
                    entry.epi_id                     as epi_id_entry,
                    entry.epi_type                   as epi_type_entry,
                    entry.analysis_id                as analysis_id_entry,
                    entry.code_system_raw            as code_system_raw_entry,
                    entry.code_system_name_raw       as code_system_name_raw_entry,
                    entry.analysis_code_raw          as analysis_code_raw_entry,
                    entry.analysis_name_raw          as analysis_name_raw_entry,
                    entry.parameter_code_raw         as parameter_code_raw_entry,
                    entry.parameter_name_raw         as parameter_name_raw_entry,
                    entry.parameter_unit_raw         as parameter_unit_raw_entry,
                    entry.reference_values_raw       as reference_values_raw_entry,
                    entry.effective_time_raw         as effective_time_raw_entry,
                    entry.value_raw                  as value_raw_entry,
                    entry.analysis_name              as analysis_name_entry,
                    entry.parameter_name             as parameter_name_entry,
                    entry.effective_time             as effective_time_entry,
                    entry.value                      as value_entry,
                    entry.parameter_unit_from_suffix as parameter_unit_from_suffix_entry,
                    entry.suffix                     as suffix_entry,
                    entry.value_type                 as value_type_entry,
                    entry.time_series_block          as time_series_block_entry,
                    entry.reference_values           as reference_values_entry,
                    entry.parameter_unit             as parameter_unit_entry,
                    entry.loinc_unit                 as loinc_unit_entry,
                    entry.substrate                  as substrate_entry,
                    entry.elabor_t_lyhend            as elabor_t_lyhend_entry,
                    entry.loinc_code                 as loinc_code_entry,
                    entry.source                     as source_entry
                from {source_html} as html
                join {source_entry} as entry
                on 
                    html.epi_id = entry.epi_id and
                    date(html.effective_time) = date(entry.effective_time) and
                    html.value = entry.value
                -- only match unresolved rows
                left join {un_html} un_html
                on un_html.id = html.id
                left join {un_entry} un_entry
                on un_entry.id = entry.id
                where un_html.id is not null or un_entry.id is not null
            )
            select * from ved_matched;
            -- PN can be different right now
            -- where parameter_name_entry is null or parameter_name_html is null;


            
            with ved_multip_entry as 
            (
                -- 1 HTML many entry
                select id_entry
                from {ved_matches}
                group by id_entry
                having count(*) >= 2
            ),
            ved_multip_html as 
            (
                 -- 1 entry many HTML
                 select id_html
                 from {ved_matches}
                 group by id_html
                 having count(*) >= 2
            )
            -- 1 on 1 relationship matches
            insert into {matched} 
            select distinct ved.id_html,
                            ved.id_entry,
                            ved.row_nr_html,
                            ved.epi_id_html,
                            ved.epi_type_html,
                            ved.parse_type_html,
                            ved.panel_id_html,
                            ved.analysis_name_raw_html,
                            ved.parameter_name_raw_html,
                            ved.parameter_unit_raw_html,
                            ved.reference_values_raw_html,
                            ved.value_raw_html,
                            ved.analysis_substrate_raw_html, -- order switched
                            ved.effective_time_raw_html, -- order switched
                            ved.analysis_name_html,
                            ved.parameter_name_html,
                            ved.effective_time_html,
                            ved.value_html,
                            ved.parameter_unit_from_suffix_html,
                            ved.suffix_html,
                            ved.value_type_html,
                            ved.time_series_block_html,
                            ved.reference_values_html,
                            ved.parameter_unit_html,
                            ved.loinc_unit_html,
                            ved.substrate_html,
                            ved.elabor_t_lyhend_html,
                            ved.loinc_code_html,
                            ved.source_html,
                            ved.original_analysis_entry_id,
                            ved.epi_id_entry,
                            ved.epi_type_entry,
                            ved.analysis_id_entry,
                            ved.code_system_raw_entry,
                            ved.code_system_name_raw_entry,
                            ved.analysis_code_raw_entry,
                            ved.analysis_name_raw_entry,
                            ved.parameter_code_raw_entry,
                            ved.parameter_name_raw_entry,
                            ved.parameter_unit_raw_entry,
                            ved.reference_values_raw_entry,
                            ved.effective_time_raw_entry,
                            ved.value_raw_entry,
                            ved.analysis_name_entry,
                            ved.parameter_name_entry,
                            ved.effective_time_entry,
                            ved.value_entry,
                            ved.parameter_unit_from_suffix_entry,
                            ved.suffix_entry,
                            ved.value_type_entry,
                            ved.time_series_block_entry,
                            ved.reference_values_entry,
                            ved.parameter_unit_entry,
                            ved.loinc_unit_entry,
                            ved.substrate_entry,
                            ved.elabor_t_lyhend_entry,
                            ved.loinc_code_entry,
                            ved.source_entry,
                            'epi_id, effective_time, value' as match_description
            from {ved_matches} as ved
            left join ved_multip_html as html_ved
            on ved.id_html = html_ved.id_html
            left join ved_multip_entry as entry_ved
            on ved.id_entry = entry_ved.id_entry
            where html_ved.id_html is null and entry_ved.id_entry is null;

            drop table {ved_matches};     
            """
            ).format(
                role=sql.Identifier(self.role),
                matched=self.tables.analysis_matched,
                ved_matches=self.tables.ved_matches,
                source_html=self.tables.source_html,
                source_entry=self.tables.source_entry,
                un_html=self.tables.unresolved_html_rows,
                un_entry=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        self.update_unresolved_rows(cur)

        cur.close()
        conn.close()
        self.mark_as_complete()


class AddUnmatchedToMatched(CDAMatchingSubtask):
    """
    Add unresolved rows to matched with match_description 'unresolved_entry' or 'unresolved_html'
    """

    source_tables = ["entry_source", "html_source", "unresolved_html_rows", "unresolved_entry_rows"]
    altered_tables = ["analysis_matched"]

    def run(self):
        self.log_current_action("Matching 11")
        self.log_schemas()
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
            insert into {matched} 
            (
                row_nr_html,
                epi_id_html,
                epi_type_html,
                parse_type_html,
                panel_id_html,
                analysis_name_raw_html,
                parameter_name_raw_html,
                parameter_unit_raw_html,
                reference_values_raw_html,
                effective_time_raw_html,
                value_raw_html,
                analysis_substrate_raw_html,
                --analysis_substrate_html,
                --entry_match_id_html,
                --entry_html_match_desc_html,
                id_html,
                analysis_name_html,
                parameter_name_html,
                effective_time_html,
                value_html,
                parameter_unit_from_suffix_html,
                suffix_html,
                value_type_html,
                time_series_block_html,
                reference_values_html,
                source_html,
                parameter_unit_html,
                loinc_unit_html,
                substrate_html,
                elabor_t_lyhend_html,
                loinc_code_html,
                match_description
            )
            select html.*, 'unresolved_html'
            from {source_html} html
            left join {un_html} un_html
            on un_html.id = html.id
            where un_html.id is not null;

            insert into {matched} 
            (
                id_entry,
                original_analysis_entry_id,
                epi_id_entry,
                epi_type_entry,
                analysis_id_entry,
                code_system_raw_entry,
                code_system_name_raw_entry,
                analysis_code_raw_entry,
                analysis_name_raw_entry,
                parameter_code_raw_entry,
                parameter_name_raw_entry,
                parameter_unit_raw_entry,
                reference_values_raw_entry,
                effective_time_raw_entry,
                value_raw_entry,
                analysis_name_entry,
                parameter_name_entry,
                effective_time_entry,
                value_entry,
                parameter_unit_from_suffix_entry,
                suffix_entry,
                value_type_entry,
                time_series_block_entry,
                reference_values_entry,
                source_entry,
                parameter_unit_entry,
                loinc_unit_entry,
                substrate_entry,
                elabor_t_lyhend_entry,
                loinc_code_entry,
                match_description
            )
            select entry.*, 'unresolved_entry'
            from {source_entry} entry
            left join {un_entry} un_entry
            on un_entry.id = entry.id
            where un_entry.id is not null;

            drop table {un_entry};
            drop table {un_html};
            """
            ).format(
                role=sql.Identifier(self.role),
                matched=self.tables.analysis_matched,
                source_html=self.tables.source_html,
                source_entry=self.tables.source_entry,
                un_html=self.tables.unresolved_html_rows,
                un_entry=self.tables.unresolved_entry_rows,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
