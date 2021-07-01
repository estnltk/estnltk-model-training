import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class CreatePatientTable(CDASubtask):

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter(default="patient")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="patient_raw")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Create Patient table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Create empty table in the right role to guarantee access rights
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists  {target_schema}.{target_table};
            create table {target_schema}.{target_table}
            (
                id SERIAL NOT NULL, 
                src_id INTEGER, 
                epi_id TEXT, 
                epi_type_raw TEXT, 
                pat_id_root_raw TEXT, 
                pat_id TEXT, 
                epi_time_raw TEXT, 
                pat_sex_raw TEXT, 
                pat_birth_time_raw TEXT, 
                hospital_name_raw TEXT, 
                hospital_code_raw TEXT, 
                hospital_addr_county_raw TEXT, 
                hospital_addr_city_raw TEXT, 
                hospital_addr_line_raw TEXT, 
                admission_code_raw TEXT, 
                admission_display_name_raw TEXT, 
                author_specialty_code_raw TEXT, 
                author_specialty_name_raw TEXT, 
                author_assigned_person_raw TEXT, 
                author_assigned_person_code_raw TEXT, 
                effective_time_low_raw TEXT, 
                effective_time_high_raw TEXT, 
                epi_time_dt_raw TIMESTAMP WITHOUT TIME ZONE, 
                effective_time_low_dt_raw TIMESTAMP WITHOUT TIME ZONE, 
                effective_time_high_dt_raw TIMESTAMP WITHOUT TIME ZONE, 
                PRIMARY KEY (id)
            );
            reset role;
            """
            ).format(
                role=sql.Literal(self.role),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        # Update table in the standard role or we might get permission errors
        cur.execute(
            sql.SQL(
                """
            insert into {target_schema}.{target_table}
            (
                src_id, 
                epi_id, 
                epi_type_raw, 
                pat_id_root_raw, 
                pat_id, 
                epi_time_raw, 
                pat_sex_raw, 
                pat_birth_time_raw, 
                hospital_name_raw, 
                hospital_code_raw, 
                hospital_addr_county_raw, 
                hospital_addr_city_raw, 
                hospital_addr_line_raw, 
                admission_code_raw, 
                admission_display_name_raw, 
                author_specialty_code_raw, 
                author_specialty_name_raw, 
                author_assigned_person_raw, 
                author_assigned_person_code_raw, 
                effective_time_low_raw, 
                effective_time_high_raw,
                epi_time_dt_raw,
                effective_time_low_dt_raw,
                effective_time_high_dt_raw
            )
            select 
                id,
                epi_id,
                epi_type,
                pat_id_root,
                pat_id,
                epi_time,
                pat_sex,
                pat_birth_time,
                hospital_name,
                hospital_code,
                hospital_addr_county,
                hospital_addr_city,
                hospital_addr_line,
                admission_code,
                admission_display_name,
                author_specialty_code,
                author_specialty_name,
                author_assigned_person,
                author_assigned_person_code,
                effective_time_low,
                effective_time_high,
                {target_schema}.clean_effective_time(a.epi_time::text),
                {target_schema}.clean_effective_time(a.effective_time_low::text),
                {target_schema}.clean_effective_time(a.effective_time_high::text)
            from {source_schema}.{source_table} a; 
            """
            ).format(
                source_schema=sql.Identifier(self.source_schema),
                source_table=sql.Identifier(self.source_table),
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
