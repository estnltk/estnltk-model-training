import luigi
from psycopg2 import sql
import time
from datetime import timedelta


from cda_data_cleaning.common.luigi_tasks import CDASubtask
from .create_loinc_mappings import CreateAllLoincMappings


class ApplyLoincMapping(CDASubtask):
    """
    Note! Requires the existance of table 'elabor_analysis' in 'classification' schema

    1. Create necessary tables for loinc mapping (CreateAllLoincMappings)
    2. Based on parameter_unit assigns loinc_unit to each row (AssignLoincUnit),
       creates new columns
        - 'parameter_unit' (cleaned parameter_unit_raw)
        - 'loinc_unit'
    3. Based on 'analysis_name' and 'parameter_name' assigns to source table substrate (mainly 'Bld' and 'Urine')
       this makes LOINC mapping more accurate,
       creates new column
        - 'substrate'
    4. In parameter_name_raw cleaning all information from parenthesis is deleted, but it is actually needed for
       accurate LOINC mapping. Therefore, bring back the information from parenthesis whether necessary
       updates column
        - 'parameter_name'
    5. Creates target table (<prefix>_analysis_html_loinced or <prefix>_analysis_entry_loinced) with new columns
        - 'loinc_code'
        - 't_lyhend'
        - 'substrate'
    6. Applies loinc_code and elabor_t_lyhend mappings (created in 1.) to source table
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter()
    target_table = luigi.Parameter()
    mapping_schema = luigi.Parameter(default="work")
    mapping_prefix = luigi.Parameter(default="")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    source_tables = [
        "source",
        "elabor_mapping_1",
        "elabor_mapping_2",
        "manual_mapping_1",
        "manual_mapping_2",
        "manual_mapping_3",
        "manual_mapping_4",
    ]
    target_tables = ["target"]

    class DatabaseTables:
        __slots__ = [
            "source",
            "target",
            "augmented_source",
            "elabor_mapping_1",
            "elabor_mapping_2",
            "manual_mapping_1",
            "manual_mapping_2",
            "manual_mapping_3",
            "manual_mapping_4",
            "manual_mapping_5",
            "panel_mapping",
            "long_loinc_mapping_unique",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_prefix = str(self.mapping_prefix) + "_" if len(str(self.mapping_prefix)) > 0 else ""

        self.tables = self.DatabaseTables()

        # Permanent tables
        self.tables.source = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(self.source_table)
        )
        self.tables.target = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier(self.target_table)
        )

        # Various mappings used to assign loinc codes
        self.tables.elabor_mapping_1 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "elabor_parameter_name_to_loinc_mapping"),
        )
        self.tables.elabor_mapping_2 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "elabor_parameter_name_unit_to_loinc_mapping"),
        )
        self.tables.manual_mapping_1 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "parameter_name_to_loinc_mapping"),
        )
        self.tables.manual_mapping_2 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "parameter_name_unit_to_loinc_mapping"),
        )
        self.tables.manual_mapping_3 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "analysis_parameter_name_to_loinc_mapping"),
        )
        self.tables.manual_mapping_4 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "analysis_parameter_name_unit_to_loinc_mapping"),
        )
        self.tables.manual_mapping_5 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "analysis_name_parameter_unit_to_loinc_mapping"),
        )
        self.tables.long_loinc_mapping_unique = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "long_loinc_mapping_unique"),
        )
        self.tables.panel_mapping = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema),
            table=sql.Identifier(self.table_prefix + "elabor_analysis_name_to_panel_loinc"),
        )

        # Temporary tables
        self.tables.augmented_source = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.schema), table=sql.Identifier("temp_" + str(self.source_table) + "_2")
        )
        self.temp_intermediate_table1 = "temp_" + str(self.source_table) + "_1"
        self.temp_intermediate_table2 = "temp_" + str(self.source_table) + "_2"

    def requires(self):

        # Make sure that all mappings for loincing are present
        task_01 = CreateAllLoincMappings(
            prefix=self.mapping_prefix,
            config_file=self.config_file,
            schema=self.schema,
            role=self.role,
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=self.requirement,
        )

        # Clean units for before loincing is done but not before source tables are generated
        task_02 = AssignLoincUnit(
            config_file=self.config_file,
            role=self.role,
            schema=self.schema,
            source_table=self.source_table,
            target_table=self.temp_intermediate_table1,
            mapping_schema=self.mapping_schema,
            mapping_table_1=self.table_prefix + "parameter_unit_to_cleaned_unit",
            mapping_table_2=self.table_prefix + "parameter_unit_to_loinc_unit_mapping",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_01,
        )

        task_03 = AssignSubstrate(
            config_file=self.config_file,
            role=self.role,
            schema=self.schema,
            source_table=self.temp_intermediate_table1,
            target_table=self.temp_intermediate_table2,
            mapping_schema=self.mapping_schema,
            long_loinc_mapping_unique=self.table_prefix + "long_loinc_mapping_unique",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_02,
        )

        task_04 = UpdateParameternameWithElabor(
            config_file=self.config_file,
            schema=self.schema,
            source_table=self.temp_intermediate_table2,
            target_table=self.temp_intermediate_table2,
            mapping_schema=self.mapping_schema,
            mapping_table=self.table_prefix + "elabor_parameter_name_to_loinc_mapping",
            luigi_targets_folder=self.luigi_targets_folder,
            requirement=task_03,
        )

        return [task_01, task_02, task_03, task_04]

    def run(self):
        self.log_current_time("Current time")
        self.log_current_action("LOINC5: Apply LOINC mapping")
        self.log_schemas()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
                -- some LOINC codes (mainly from HMTL) are situated in parameter_name column together with the actual pn
                -- this query extracts the loinc codes and places it under loinc_code column
                
                set role {role};
                
                -- creating target table (analysis_*_loinced) with extra columns for t_lyhend and loinc 
                -- extract the loinc code from parameter_name column, rest of the loinc codes will be NULL for now
                drop table if exists {target_table};
                create table {target_table} as
                select *,
                       null as elabor_t_lyhend,
                       case when parameter_name ~ '^\d{{3,}}-\d' then
                                split_part(parameter_name, ' ', 1) end as loinc_code
                    from {source_table};
                reset role;
    
                -- loinc code is now extracted from parameter_name
                -- remove the loinc code from parameter_name column
                -- assign t_lyhend to the given loinc_code
                update {target_table}
                set parameter_name = regexp_replace(parameter_name, '^\d{{3,}}-\d\s*', ''),
                   elabor_t_lyhend = t_lyhend
                from classifications.elabor_analysis elab
                where loinc_code = elab.loinc_no;
                """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )

        # Create indices on target table
        cur.execute(
            sql.SQL(
                """
                -- add md5 for unbounded columns, otherwise "index row size x exceeds maximum 2712 for index"
                -- https://github.com/doorkeeper-gem/doorkeeper/wiki/How-to-fix-PostgreSQL-error-on-index-row-size
                create index if not exists {index_analysis_name} on {target_table}(md5(upper(analysis_name)));
                create index if not exists {index_parameter_name} on {target_table}(md5(upper(parameter_name)));
                create index if not exists {index_parameter_unit} on {target_table}(md5(upper(parameter_unit)));
                create index if not exists {index_substrate} on {target_table}(md5(upper(substrate)));
                """
            ).format(
                target_table=self.tables.target,
                index_analysis_name=sql.Identifier(self.table_prefix + "_analysis_name"),
                index_parameter_name=sql.Identifier(self.table_prefix + "_parameter_name"),
                index_parameter_unit=sql.Identifier(self.table_prefix + "_parameter_unit"),
                index_substrate=sql.Identifier(self.table_prefix + "_substrate"),
            )
        )

        ##########################################################################################################
        # check if table has column 'parameter_code_raw' (basically only for entry table)
        # if yes, use it for loinc code mapping (more accurate information than analysis_code_raw)
        # note that the column can either contain loinc_code or elabor t_lyhend or 'ANA'
        # 'ANA' means that information is analysis_code_raw column

        is_parameter_code_raw = self.db_table_column_exists(conn, self.schema, self.source_table, "parameter_code_raw")

        if is_parameter_code_raw:
            cur.execute(
                sql.SQL(
                    """
                    set search_path to {schema};

                    -- elabor mapping based on parameter code
                    update {target_table} as target
                    set loinc_code      = case
                                              when parameter_code_raw ~ '^\d{{3,}}-\d' then parameter_code_raw
                                              when parameter_code_raw is not null and parameter_code_raw != 'ANA' then mapping.loinc_code 
                                          end,
                        elabor_t_lyhend = case
                                              when parameter_code_raw ~ '^\d{{3,}}-\d' then mapping.t_lyhend
                                              when parameter_code_raw is not null and parameter_code_raw != 'ANA' then parameter_code_raw
                                          end
                    from {elabor_mapping_table_1} as mapping
                    where target.loinc_code is null
                      and (upper(target.parameter_code_raw) = upper(mapping.parameter_name) or
                           upper(target.parameter_code_raw) = upper(mapping.loinc_code));
                    
                    
                    -- manual mapping based on parameter_code_raw
                    update {target_table} as target
                    set loinc_code      = case
                                              when parameter_code_raw ~ '^\d{{3,}}-\d' then parameter_code_raw
                                              when parameter_code_raw is not null and parameter_code_raw != 'ANA' then mapping.loinc_code
                                          end,
                        elabor_t_lyhend = case
                                              when parameter_code_raw ~ '^\d{{3,}}-\d' then mapping.t_lyhend 
                                              when parameter_code_raw is not null and parameter_code_raw != 'ANA' then parameter_code_raw
                                          end
                    from {manual_mapping_table_1} as mapping
                    where target.loinc_code is null
                      and (upper(target.parameter_code_raw) = upper(mapping.parameter_name) or
                           upper(target.parameter_code_raw) = upper(mapping.loinc_code));            
                """
                ).format(
                    schema=sql.Identifier(self.schema),
                    target_table=self.tables.target,
                    elabor_mapping_table_1=self.tables.elabor_mapping_1,
                    manual_mapping_table_1=self.tables.manual_mapping_1,
                )
            )

        conn.commit()

        cur.execute(
            sql.SQL(
                """
            set role {role};
            set search_path to {schema};
            
            ---------------------------------------------------------------------------------------------------------------------
            -- Mapping with substrate (Bld/Urine etc)
            ---------------------------------------------------------------------------------------------------------------------
            -- elabor mapping based on parameter_name, parameter_unit and substrate
            
            update {target_table} as target
            set loinc_code = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
            from {elabor_mapping_table_2} as mapping
            where target.loinc_code is null
              and upper(target.parameter_name) = upper(mapping.parameter_name) and
                  upper(target.substrate) = upper(mapping.substrate) and
                  ( -- parameter_unit
                          upper(target.loinc_unit) = upper(mapping.t_yhik) or
                          upper(target.parameter_unit) = upper(mapping.t_yhik) or
                          upper(target.parameter_unit_from_suffix) = upper(mapping.t_yhik)
                      );"""
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """     
            ---------------------------------------------------------------------------------------------------------------------
            -- elabor mapping based on parameter_name and substrate
            update {target_table} as target
            set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
            from {elabor_mapping_table_1} as mapping
            where target.loinc_code is null
              and upper(target.parameter_name) = upper(mapping.parameter_name)
              and upper(target.substrate) = upper(mapping.substrate);
            """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on analysis_name, parameter_name, parameter_unit and substrate
            
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                    elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_4} as mapping
                where target.loinc_code is null
                      and upper(target.parameter_name) = upper(mapping.parameter_name)
                      and upper(target.substrate) = upper(mapping.substrate)
                      and upper(target.analysis_name) = upper(mapping.analysis_name) 
                      and (upper(target.parameter_unit) = upper(mapping.parameter_unit) or
                           upper(target.loinc_unit) = upper(mapping.parameter_unit) or
                           upper(target.parameter_unit_from_suffix) = upper(mapping.parameter_unit)
                    );  """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """   
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on parameter_name, parameter_unit and substrate
                    
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                    elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_2} as mapping
                where target.loinc_code is null
                  and upper(target.parameter_name) = upper(mapping.parameter_name)
                  and upper(target.substrate) = upper(mapping.substrate)
                  and (
                        upper(target.parameter_unit) = upper(mapping.parameter_unit) or
                        upper(target.loinc_unit) = upper(mapping.parameter_unit) or
                        upper(target.parameter_unit_from_suffix) = upper(mapping.parameter_unit)
                    );"""
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on analysis_name, parameter_name and substrate
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_3} as mapping
                where target.loinc_code is null
                  and upper(target.parameter_name) = upper(mapping.parameter_name)
                  and upper(target.substrate) = upper(mapping.substrate)
                  and upper(target.analysis_name) = upper(mapping.analysis_name);
                  """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on parameter_name and substrate
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_1} as mapping
                where target.loinc_code is null
                and upper(target.parameter_name) = upper(mapping.parameter_name)
                and upper(target.substrate) = upper(mapping.substrate);
                """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on analysis_name, parameter_unit, substrate
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_5} as mapping
                where target.loinc_code is null
                    and upper(target.analysis_name) = upper(mapping.analysis_name)
                    and upper(target.substrate) = upper(mapping.substrate)
                    and (
                    upper(target.parameter_unit) = upper(mapping.parameter_unit) or
                    upper(target.loinc_unit) = upper(mapping.parameter_unit) or
                    upper(target.parameter_unit_from_suffix) = upper(mapping.parameter_unit)
                );                 """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                manual_mapping_table_5=self.tables.manual_mapping_5,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- Mapping withOUT substrate (Bld/Urine etc)
                ---------------------------------------------------------------------------------------------------------------------
                
                ---------------------------------------------------------------------------------------------------------------------
                -- elabor mapping based on parameter_name, parameter_unit
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {elabor_mapping_table_2} mapping
                where target.loinc_code is null
                and upper(target.parameter_name) = upper(mapping.parameter_name)
                and (
                upper(target.loinc_unit) = upper(mapping.t_yhik) or
                upper(target.parameter_unit) = upper(mapping.t_yhik) or
                upper(target.parameter_unit_from_suffix) = upper(mapping.t_yhik)
                )
                --substrates must not be contradicting
                and (target.substrate is null or
                mapping.substrate is null or
                upper(target.substrate) = upper(mapping.substrate));
                """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
            ---------------------------------------------------------------------------------------------------------------------
            -- elabor mapping based on parameter_name
            update {target_table} as target
            set loinc_code      = mapping.loinc_code,
            elabor_t_lyhend = mapping.t_lyhend
            from {elabor_mapping_table_1} as mapping
            where target.loinc_code is null
            and upper(target.parameter_name) = upper(mapping.parameter_name)
            --substrates must not be contradicting
            and (target.substrate is null or
            mapping.substrate is null or
            upper(target.substrate) = upper(mapping.substrate));
            ;
            """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on analysis_name, parameter_name, parameter_unit
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_4} as mapping
                where target.loinc_code is null
                and upper(target.parameter_name) = upper(mapping.parameter_name)
                and upper(target.analysis_name) = upper(mapping.analysis_name)
                and (
                upper(target.parameter_unit) = upper(mapping.parameter_unit) or
                upper(target.loinc_unit) = upper(mapping.parameter_unit) or
                upper(target.parameter_unit_from_suffix) = upper(mapping.parameter_unit)
                );                 """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on parameter_name, parameter_unit
                
                --update {target_table}
                --set parameter_name_raw = translate(parameter_name_raw, 'ä,ö,õ,ü,Ä,Ö,Õ,Ü', 'a,o,o,u,A,O,O,U');
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_2} as mapping
                where target.loinc_code is null
                    and upper(target.parameter_name) = upper(mapping.parameter_name)
                    and (
                    upper(target.parameter_unit) = upper(mapping.parameter_unit) or
                    upper(target.loinc_unit) = upper(mapping.parameter_unit) or
                    upper(target.parameter_unit_from_suffix) = upper(mapping.parameter_unit)
                    )
                    --substrates must not be contradicting
                    and (target.substrate is null or
                    mapping.substrate is null or
                    upper(target.substrate) = upper(mapping.substrate));
                """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on analysis_name, parameter_name
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_3} as mapping
                where target.loinc_code is null
                and upper(target.parameter_name) = upper(mapping.parameter_name)
                and upper(target.analysis_name) = upper(mapping.analysis_name);
                """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on parameter_name
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_1} as mapping
                where target.loinc_code is null
                and upper(target.parameter_name) = upper(mapping.parameter_name)
                --substrates must not be contradicting
                and (target.substrate is null or
                mapping.substrate is null or
                upper(target.substrate) = upper(mapping.substrate));"""
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on analysis_name, parameter_unit
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_5} as mapping
                where target.loinc_code is null
                    and upper(target.analysis_name) = upper(mapping.analysis_name)
                    and (
                    upper(target.parameter_unit) = upper(mapping.parameter_unit) or
                    upper(target.loinc_unit) = upper(mapping.parameter_unit) or
                    upper(target.parameter_unit_from_suffix) = upper(mapping.parameter_unit)
                );                 """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                manual_mapping_table_5=self.tables.manual_mapping_5,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- mapping panels based on analysis name
                --------------------------------------------------------------------------------------------------------------------
                -- do not need substrate, because analysis_name tells whether it is urine or blood
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {panel_mapping} as mapping
                where target.loinc_code is null
                and upper(target.analysis_name) = upper(mapping.analysis_name);
                """
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        cur.execute(
            sql.SQL(
                """
                ---------------------------------------------------------------------------------------------------------------------
                -- manual mapping based on analysis_name (mainly because parameter name has been placed under analysis name)
                
                update {target_table} as target
                set loinc_code      = mapping.loinc_code,
                elabor_t_lyhend = mapping.t_lyhend
                from {manual_mapping_table_1} as mapping
                    where target.loinc_code is null
                    and upper(target.analysis_name) = upper(mapping.parameter_name)
                    --substrates must not be contradicting
                    and (target.substrate is null or
                    mapping.substrate is null or
                    upper(target.substrate) = upper(mapping.substrate));"""
            ).format(
                role=sql.Literal(self.role),
                schema=sql.Identifier(self.schema),
                source_table=self.tables.augmented_source,
                target_table=self.tables.target,
                elabor_mapping_table_1=self.tables.elabor_mapping_1,
                elabor_mapping_table_2=self.tables.elabor_mapping_2,
                manual_mapping_table_1=self.tables.manual_mapping_1,
                manual_mapping_table_2=self.tables.manual_mapping_2,
                manual_mapping_table_3=self.tables.manual_mapping_3,
                manual_mapping_table_4=self.tables.manual_mapping_4,
                panel_mapping=self.tables.panel_mapping,
            )
        )
        conn.commit()

        ##########################################################################################################
        # check if table has column 'analysis_code_raw' (basically only for entry table)
        # if yes, use it for loinc code mapping
        # note that the column can either contain loinc_code or elabor t_lyhend
        # however, it often contains panel loinc code, so it is very general mapping

        is_analysis_code_raw = self.db_table_column_exists(conn, self.schema, self.source_table, "analysis_code_raw")

        if is_analysis_code_raw:
            cur.execute(
                sql.SQL(
                    """
                    set search_path to {schema};

                    -- elabor mapping based on analysis_code_raw
                    update {target_table} as target
                    set  loinc_code      = case when analysis_code_raw ~ '^\d{{3,}}-\d' then analysis_code_raw
                                                when analysis_code_raw is not null then mapping.loinc_code
                                           end,
                        elabor_t_lyhend = case when analysis_code_raw ~ '^\d{{3,}}-\d' then mapping.t_lyhend
                                               when analysis_code_raw is not null then analysis_code_raw
                                          end
                    from {elabor_mapping_table_1} as mapping
                    where target.loinc_code is null
                          and (upper(target.analysis_code_raw) = upper(mapping.parameter_name) or
                               upper(target.analysis_code_raw) = upper(mapping.loinc_code));
                    
                    
                    -- manual mapping based on analysis_code_raw
                    update {target_table} as target
                    set  loinc_code      = case when analysis_code_raw ~ '^\d{{3,}}-\d' then analysis_code_raw
                                                when analysis_code_raw is not null then mapping.loinc_code
                                           end,
                        elabor_t_lyhend = case when analysis_code_raw ~ '^\d{{3,}}-\d' then mapping.t_lyhend
                                               when analysis_code_raw is not null then analysis_code_raw
                                          end
                    from {manual_mapping_table_1} as mapping
                    where target.loinc_code is null
                          and (upper(target.analysis_code_raw) = upper(mapping.parameter_name) or
                               upper(target.analysis_code_raw) = upper(mapping.loinc_code));  
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    target_table=self.tables.target,
                    elabor_mapping_table_1=self.tables.elabor_mapping_1,
                    manual_mapping_table_1=self.tables.manual_mapping_1,
                )
            )

        conn.commit()
        ##########################################################################################################

        # Delete intermediate table
        cur.execute(
            sql.SQL(
                """
                set role {role};
                drop table {temp_table0};
                drop table {temp_table1};
                """
            ).format(
                schema=sql.Identifier(self.schema),
                role=sql.Literal(self.role),
                temp_table0=self.tables.augmented_source,
                temp_table1=sql.Identifier(self.temp_intermediate_table1),
            )
        )
        conn.commit()

        # check that source and target table have exactly the same number of rows
        cur.execute(
            sql.SQL(
                """
                    select count(*) from {source_table};
                    """
            ).format(schema=sql.Identifier(self.schema), source_table=self.tables.source)
        )
        nr_source_rows = cur.fetchone()[0]

        cur.execute(
            sql.SQL(
                """
                    select count(*) from {target_table};
                    """
            ).format(
                schema=sql.Identifier(self.schema), target_table=self.tables.target,
            )
        )
        nr_target_rows = cur.fetchone()[0]

        if nr_source_rows != nr_target_rows:
            print("\n\n############################################################################")
            print("############################################################################")
            print("WARNING:")
            print(
                self.tables.source,
                " and ",
                self.tables.target,
                "have different number of rows",
                "(",
                nr_source_rows,
                "!=",
                nr_target_rows,
                ")",
            )
            print(
                "This means that some rows in analysis_html_cleaned/analysis_entry_cleaned get multiple loinc codes!"
            )
            print("############################################################################")
            print("############################################################################\n\n")

        cur.close()
        conn.close()
        self.mark_as_complete()


class AssignLoincUnit(CDASubtask):
    """
    Cleans analysis units and assigns a specific loinc_unit used further in loinc mapping.
    Assumes that all necessary mapping tables are created in mapping schema that can coincide with work schema.

    Can be applied to any table that has columns:
    - parameter_unit_raw
    - parameter_unit_from_suffix

    Copies the entire source table to the target table and adds columns:
    - parameter_unit
    - loinc_unit
    based on the source table columns:
    - parameter_unit_raw
    - parameter_unit_from_suffix

    How do you guarantee that each row is mapped to a single row and not into several rows if there is an 'or'
    clause in the mapping. How do you resolve conflicts?

    There are no conflicts between parameter_unit_raw and parameter_unit_from_suffix.
    This is guaranteed by code found in cleaning functions
    analysis_data_cleaning/step00_create_cleaning_functions/psql_cleaning_functions/value_cleaning_functions/clean_value_functions.psql

    When parameter_unit_raw is NOT NULL then parameter_unit_from_suffix will be a copy of parameter_unit_raw
        --> no conflict beacuse both are same
    Only when parameter_unit_raw is NULL then unit is being searched from value_raw column.
        - if there is no unit in value_raw, then parameter_unit_from_suffix will be a copy of parameter_unit_raw (NULL)
            --> no conflict because both are NULL
        - if there is a unit in value_raw, then this unit will be moved under parameter_unit_raw
            --> no conflict because parameter_unit_raw is NULL and parameter_unit_raw is NOT NULL

    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter()
    target_table = luigi.Parameter()
    mapping_schema = luigi.Parameter(default="work")
    mapping_table_1 = luigi.Parameter(default="parameter_unit_to_cleaned_unit")
    mapping_table_2 = luigi.Parameter(default="parameter_unit_to_loinc_unit_mapping")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        self.log_current_time("Current time")
        self.log_current_action("LOINC2: Assign Loinc Unit")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        mapping_1 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(self.mapping_table_1)
        )
        mapping_2 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(self.mapping_table_2)
        )

        cur.execute(
            sql.SQL(
                """
            set role {role};
            set search_path to {schema};
            
            drop table if exists {target_table};
            create table {target_table} as 
            with mappings as
            (
                select * from {mapping_1}
                union
                select * from {mapping_2}
            )
            select 
                source.*, 
                mappings.parameter_unit_clean as parameter_unit, 
                mappings.loinc_unit 
            from {source_table} as source
            left join mappings  
            on 
                source.parameter_unit_raw = mappings.parameter_unit or 
                source.parameter_unit_from_suffix = mappings.parameter_unit;
                
            reset role;
            
            update {target_table}
                set parameter_unit = NULL
                where parameter_unit = '';
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.schema),
                source_table=sql.Identifier(self.source_table),
                target_table=sql.Identifier(self.target_table),
                mapping_1=mapping_1,
                mapping_2=mapping_2,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class AssignSubstrate(CDASubtask):
    """
    Assigns to each row substrate (Bld, Urine, Ser/Plas, etc)
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter()
    target_table = luigi.Parameter()
    mapping_schema = luigi.Parameter(default="work")
    long_loinc_mapping_unique = luigi.Parameter(default="long_loinc_mapping_unique")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        self.log_current_time("Current time")
        self.log_current_action("LOINC3: Assign Substrate")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        long_loinc_mapping_unique = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(self.long_loinc_mapping_unique)
        )

        cur.execute(
            sql.SQL(
                """
                set role {role};
                set search_path to {schema};
    
                drop table if exists {target_table};
                create table {target_table} as
                select source.*, ll.substrate as substrate
                from {source_table} source
                         left join {long_loinc_mapping_unique} ll on
                    source.analysis_name = ll.analysis_name and
                    source.parameter_name = ll.parameter_name;
                reset role;
                
                update {target_table}
                set substrate = case
                    -- do not know if blood or urine
                    when lower(analysis_name) ~ 'hematoloogilised ja uriini uuringud' then lower(substrate)
                    when lower(analysis_name) ~ 'uriin' then 'urine'
                    when lower(analysis_name) ~ ('seerumis|plasma') or lower(parameter_name) ~ ('seerumis|plasma')
                        then 'ser/plas'
                    when lower(analysis_name) ~ ('veri|vere|hemogramm|hemato') then 'bld'
                    else lower(substrate)
                end;
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.schema),
                source_table=sql.Identifier(self.source_table),
                target_table=sql.Identifier(self.target_table),
                long_loinc_mapping_unique=long_loinc_mapping_unique,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()


class UpdateParameternameWithElabor(CDASubtask):
    """
    During parameter_name_raw cleaning all information from parenthesis is deleted, but it is actually needed for
    more accurate LOINC mapping. Therefore, bring back the information from parenthesis whether necessary.

    E.g. Initially parameter_name_raw = pO2 (sO2 50%). This has corresponding
            - t_lyhend    aB-p50,  vB-p50,  mvB-p50, cB-p50
            - loinc_code  19214-6, 19216-1, 19217-9, 19215-3
        After cleaning parameter_name = pO2. This has corresponding
            - t_lyhend    aB-pO2, vB-pO2, mvB-pO2, cB-pO2
            - loinc_code  2703-7, 2705-2, 19211-2, 2704-5

    Therefore cleaning loses information and this step fixes it.
    """

    config_file = luigi.Parameter()
    schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter()
    target_table = luigi.Parameter()
    mapping_schema = luigi.Parameter(default="work")
    mapping_table = luigi.Parameter(default="elabor_parameter_name_to_loinc_mapping")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        self.log_current_time("Current time")
        self.log_current_action("LOINC4: Update Parametername With Elabor")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        elabor_mapping1 = sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.mapping_schema), table=sql.Identifier(self.mapping_table)
        )

        cur.execute(
            sql.SQL(
                """
                set search_path to {schema};
                
                update {target} maac
                set parameter_name = ea.kasutatav_nimetus
                from  {elabor_mapping} ea
                    -- left side: inner replace removes a1234 combinations, outer replace removes whitespaces
                    -- right side: removes whitespaces (takes only numbers and letters)
                where	regexp_replace(regexp_replace(lower(maac.parameter_name_raw), 'a\d+', '','g'),'[^a-z0-9]' ,'','g') =
                        (regexp_replace(lower(ea.kasutatav_nimetus),'[^a-z0-9]' ,'','g'))
                    -- cases where parameter_name_raw might have been excessively cleaned
                    and parameter_name_raw like '%(%'
                    and ea.kasutatav_nimetus is not null;
            """
            ).format(
                schema=sql.Identifier(self.schema),
                # target and source are same
                target=sql.Identifier(self.target_table),
                elabor_mapping=elabor_mapping1,
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
