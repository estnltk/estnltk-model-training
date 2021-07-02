import luigi
from lxml import html
from psycopg2 import sql
from datetime import datetime

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import CDASubtask
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import TableBuffer
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.data_cleaning.analysis_data_cleaning import AnalysisHtmlTable


class ParseAnalysisHtmlTable(CDASubtask):
    """
    Parsing information from html format (table original.analysis column html) to structured database table.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter(default="analysis")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="analysis_html")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        self.log_current_action("Parse Analysis Html Table")
        self.log_schemas()
        # self.log_dependencises("Working with tables {tbl1}, {tbl2} and {tbl3} \n".format(
        #         tbl1=new_table_name, tbl2=new_table_name_log, tbl3=new_table_name_meta))

        # Fix columns of corresponding tables
        col_list = [
            "row_nr",
            "epi_id",
            "panel_id",
            "analysis_name_raw",
            "parameter_name_raw",
            "parameter_unit_raw",
            "reference_values_raw",
            "effective_time_raw",
            "value_raw",
            "analysis_substrate_raw",
        ]
        col_list_log = ["time", "epi_id", "type", "message"]
        col_list_meta = ["epi_id", "attribute", "value"]

        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        buffer_size = 1000
        block_size = 10000

        tb_analysis = TableBuffer(
            conn=conn,
            schema=self.target_schema,
            table_name=str(self.target_table),
            buffer_size=buffer_size,
            column_names=col_list,
            input_type="pd",
        )
        tb_log = TableBuffer(
            conn=conn,
            schema=self.target_schema,
            table_name=str(self.target_table) + "_log",
            buffer_size=buffer_size,
            column_names=col_list_log,
            input_type="dict",
        )
        tb_meta = TableBuffer(
            conn=conn,
            schema=self.target_schema,
            table_name=str(self.target_table) + "_meta",
            buffer_size=buffer_size,
            column_names=col_list_meta,
            input_type="dict",
        )

        cur.execute(
            sql.SQL(
                """
            select html, epi_id, epi_type 
            from {schema}.{table}
            """
            ).format(schema=sql.Identifier(self.source_schema), table=sql.Identifier(self.source_table))
        )

        # Loop over the data
        rows = cur.fetchmany(block_size)
        while rows:
            for row in rows:
                (html_str, epi_id, epi_type) = row

                if html_str is None or html_str == "":
                    # print('Table is empty')
                    continue

                html_el = html.fromstring(html_str)

                # html contains only one table
                if html_el.tag == "table":
                    html_tables = [html_el]

                # html contains multiple tables
                elif html_el.tag == "div":
                    html_tables = [html_table for html_table in html_el]
                    # if html_el.tag == "table"] <- some information goes missing

                # if html does not contain table(s) then it is logged
                else:
                    print("{} does not contain html table(s)".format(html.tostring(html_el)))
                    tb_log.append(
                        [
                            {
                                "time": str(datetime.now()),
                                "epi_id": epi_id,
                                "type": "error:",
                                "message": "does not contain html table(s)",
                            }
                        ]
                    )
                    continue

                # parsing tables
                for panel_id, html_table in enumerate(html_tables):
                    parsed_data = AnalysisHtmlTable(html_table, epi_id, epi_type, panel_id)

                    # parsing failed
                    if not parsed_data.status:
                        print("Parsing failed for epi_id {} with error: {}".format(epi_id, parsed_data.error))
                        tb_log.append(
                            [
                                {
                                    "time": str(datetime.now()),
                                    "epi_id": epi_id,
                                    "type": "error:",
                                    "message": parsed_data.error,
                                }
                            ]
                        )
                        continue

                    tb_analysis.append(parsed_data.df)

                    # log row (columns): ['time', 'epi_id', 'type', 'message']
                    tb_log.append(
                        [
                            {
                                "time": str(datetime.now()),
                                "epi_id": epi_id,
                                "type": "parse_type:",
                                "message": parsed_data.parse_type,
                            }
                        ]
                    )

                    # log error
                    if parsed_data.error:
                        tb_log.append(
                            [
                                {
                                    "time": str(datetime.now()),
                                    "epi_id": epi_id,
                                    "type": "parse_type:",
                                    "message": parsed_data.error,
                                }
                            ]
                        )

                    # meta table columns: ['epi_id', 'attributes', 'value'], exists only for parse type 4 and 5
                    if parsed_data.meta:
                        tb_meta.append([{"epi_id": epi_id, "attribute": "Description:", "value": parsed_data.meta}])

            rows = cur.fetchmany(block_size)

        tb_analysis.flush()
        tb_log.flush()
        tb_meta.flush()

        # tables in wide table (parse type 1 and 3) have a lot of rows with no values (value = '-') so we can remove them
        # in some cases long tables (type 2) have value = '-' then in log is a warning
        cur.execute(
            sql.SQL(
                """
            DELETE FROM {schema}.{table}
            WHERE value_raw = '-';
            """
            ).format(schema=sql.Identifier(self.target_schema), table=sql.Identifier(self.target_table))
        )
        conn.commit()

        cur.close()
        conn.close()

        tb_analysis.close()
        tb_log.close()
        tb_meta.close()
        self.mark_as_complete()
