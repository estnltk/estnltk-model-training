import luigi
import time
from tqdm import tqdm
from psycopg2 import sql
from datetime import timedelta

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning import TableBuffer

from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.fact_extraction import (
    Type1Parser,
    Type2Parser,
    Type3Parser,
    Type4Parser,
    Type6Parser,
    Type7Parser,
)
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.common.luigi_tasks.cda_batch_task import CDABatchTask

# export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# sourceschema="work"
# sourcetable="run_202012111200_analysis_texts"
# targetschema="work"
# targettable="run_202012111200_analysis_texts_structured"
# conf="configurations/egcut_epi.ini"
# luigi --scheduler-port 8082 --module luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.analysis_data_cleaning.analysis_printout_extraction.development.extract_analysis_structured_text ExtractStructuredTextDev --config=$conf --role=egcut_epi_work_create --source-schema=$sourceschema --source-table=$sourcetable --target-schema=$targetschema --target-table=$targettable --workers=1 --log-level=INFO


class ExtractStructuredPrintoutDev(CDABatchTask):
    """
    Searches for analysis texts in source table's 'text' field and extract them to target table.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="run_202028101016_analysis_from_procedures")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="run_202011261008_analysis_texts_structured")
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):
        return []

    def run(self):
        print("-------------------------Extract structured text dev---------------------------------------------")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.itersize = 10000  # server side cursor
        block_size = 10000  # how many rows we fetch from the database
        buffer_size = 10000  # how many rows we send to database at once

        cur.execute(
            sql.SQL(
                """
                    select epi_id, table_text, source_schema, source_table, source_column,
                    source_table_id, text_type, span_start, span_end from {schema}.{source}
                    where text_type = 'type3';
                    --where epi_id = '10105478' or epi_id = '12633303' or epi_id = '35456735' or
                    --      epi_id = '42298761' or epi_id = '13679831' or epi_id = '4841144'
                """
            ).format(
                schema=sql.Identifier(self.source_schema), source=sql.Identifier(self.source_table),
            )
        )

        conn.commit()

        type1_parser = Type1Parser()
        type2_parser = Type2Parser()
        type3_parser = Type3Parser()
        type4_parser = Type4Parser()
        type6_parser = Type6Parser()
        type7_parser = Type7Parser()

        target_cols = [
            "epi_id",
            "analysis_name_raw",
            "parameter_name_raw",
            "value_raw",
            "parameter_unit_raw",
            "effective_time_raw",
            "reference_values_raw",
            "text_raw",
        ]
        # , 'text_type', 'source_schema', 'source_table', 'source_column', 'source_table_id']

        tb = TableBuffer(
            conn=conn,
            schema=self.target_schema,
            table_name=self.target_table,
            buffer_size=buffer_size,
            column_names=target_cols,
            input_type="pd",
        )

        start = time.time()
        iterations = 0

        while True:
            rows = cur.fetchmany(block_size)

            # no more rows left
            if not rows:
                break

            for i, row in tqdm(enumerate(rows)):
                iterations += 1

                epi_id = row[0]
                text = row[1]
                source_schema = row[2]
                source_table = row[3]
                source_column = row[4]
                source_table_id = row[5]
                text_type = row[6]

                if text_type == "type5":
                    continue

                type_parser = eval(text_type + "_parser")

                # print(i, "type", text_type, epi_id)
                result = type_parser(text, epi_id)

                # add information about source
                result["text_type"] = text_type
                result["source_schema"] = source_schema
                result["source_table"] = source_table
                result["source_column"] = source_column
                result["source_table_id"] = source_table_id

                tb.append(result)
            print("Processed ", iterations, "rows, time", str(timedelta(seconds=time.time() - start)))

        # flush the remaining rows
        tb.flush()
        print("\n Total running time ", str(timedelta(seconds=time.time() - start)), "\n")
        tb.close()
        print("-" * 100)

        self.mark_as_complete()
