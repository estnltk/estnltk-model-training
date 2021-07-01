import luigi
from psycopg2 import sql

from cda_data_cleaning.common.luigi_tasks import CDASubtask


class CleanEpiTimesTable(CDASubtask):
    config_file = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="epi_times")
    luigi_targets_folder = luigi.Parameter(default=".")
    min_date = luigi.Parameter(default="2001-01-01 00:00:00.000000")
    max_date = luigi.Parameter(default="2040-01-01 00:00:00.000000")

    def run(self):
        self.log_current_action("Clean epi_times table")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # Insert into epi_times values, taken from cleaning raw rows in epi_times.
        # Different than the previous SQLAlchemy version for cases where ES is moved so
        # for example, when dates (8, 7, 8, 8) are supposed to be cleaned into (7, 7, 8, 8), they
        # are currently cleaned into (8, 8, 8, 8) instead.

        # Cleaning epi_start column - value stays the same
        cur.execute(
            sql.SQL(
                """
            UPDATE {target_schema}.{target_table}
            SET
                epi_start = effective_time_low_dt_raw
            WHERE effective_time_low_dt_raw > {min_date} and effective_time_low_dt_raw < {max_date};
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                min_date=sql.Literal(self.min_date),
                max_date=sql.Literal(self.max_date)
            )
        )
        conn.commit()

        # Cleaning treatment_start column
        cur.execute(
            sql.SQL(
                """
            UPDATE {target_schema}.{target_table}
            SET
                treatment_start = greatest(epi_start, first_visit_raw)
            WHERE first_visit_raw > {min_date} and first_visit_raw < {max_date};
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                min_date=sql.Literal(self.min_date),
                max_date=sql.Literal(self.max_date)
            )
        )
        conn.commit()

        # Cleaning treatment_end column
        cur.execute(
            sql.SQL(
                """
            UPDATE {target_schema}.{target_table}
            SET
                treatment_end = greatest(treatment_start, last_visit_raw)
            WHERE last_visit_raw > {min_date} and last_visit_raw < {max_date};
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                min_date=sql.Literal(self.min_date),
                max_date=sql.Literal(self.max_date)
            )
        )
        conn.commit()

        # Cleaning epi_end column
        cur.execute(
            sql.SQL(
                """
            UPDATE {target_schema}.{target_table}
            SET
                epi_end = greatest(treatment_end, effective_time_high_dt_raw)
            WHERE effective_time_high_dt_raw > {min_date} and effective_time_high_dt_raw < {max_date};
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                min_date=sql.Literal(self.min_date),
                max_date=sql.Literal(self.max_date)
            )
        )
        conn.commit()

        # Filling applyed_cleaning column based on what was changed
        cur.execute(
            sql.SQL(
                """
            UPDATE {target_schema}.{target_table}
            SET
                applyed_cleaning =
                            CONCAT
                            (
                                CASE WHEN 
                                effective_time_low_dt_raw < {min_date}
                                OR
                                effective_time_low_dt_raw > {max_date}
                                THEN 'Invalid effective time low dt raw; ' ELSE '' END,
                                
                                CASE WHEN 
                                first_visit_raw < {min_date}
                                OR
                                first_visit_raw > {max_date}
                                THEN 'Invalid first visit raw; ' ELSE '' END,
                                
                                CASE WHEN 
                                last_visit_raw < {min_date}
                                OR
                                last_visit_raw > {max_date}
                                THEN 'Invalid last visit raw; ' ELSE '' END,
                                
                                CASE WHEN 
                                effective_time_high_dt_raw < {min_date}
                                OR
                                effective_time_high_dt_raw > {max_date}
                                THEN 'Invalid effective time high dt raw; ' ELSE '' END,
                                
                                CASE WHEN
                                reporting_time_raw < {min_date}
                                OR
                                reporting_time_raw > {max_date}
                                THEN 'Invalid reporting time raw' ELSE '' END,
                                
                                CASE WHEN effective_time_low_dt_raw != epi_start THEN 'ES changed; ' ELSE '' END,
                                CASE WHEN first_visit_raw != treatment_start THEN 'TS changed; ' ELSE '' END,
                                CASE WHEN last_visit_raw != treatment_end THEN 'TE changed; ' ELSE '' END,
                                CASE WHEN effective_time_high_dt_raw != epi_end THEN 'EE changed; ' ELSE '' END
                            );
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                min_date=sql.Literal(self.min_date),
                max_date=sql.Literal(self.max_date)
            )
        )
        conn.commit()

        # Filling in reporting_time column
        cur.execute(
            sql.SQL(
                """
            UPDATE {target_schema}.{target_table}
            SET
                reporting_time = reporting_time_raw
            WHERE reporting_time_raw > {min_date} and reporting_time_raw < {max_date};
            """
            ).format(
                target_schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
                min_date=sql.Literal(self.min_date),
                max_date=sql.Literal(self.max_date)
            )
        )
        conn.commit()

        cur.close()
        conn.close()
        self.mark_as_complete()
