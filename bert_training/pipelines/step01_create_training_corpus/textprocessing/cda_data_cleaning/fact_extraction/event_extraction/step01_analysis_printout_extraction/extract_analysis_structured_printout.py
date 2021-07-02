import luigi
import time
from tqdm import tqdm
from psycopg2 import sql
from datetime import timedelta

from cda_data_cleaning.common.table_buffer import TableBuffer
from .analysis_text_parser.type1 import Type1Parser
from .analysis_text_parser.type2 import Type2Parser
from .analysis_text_parser.type3 import Type3Parser
from .analysis_text_parser.type4 import Type4Parser
from .analysis_text_parser.type6 import Type6Parser
from .analysis_text_parser.type7 import Type7Parser
from cda_data_cleaning.common.luigi_tasks.cda_subtask import CDASubtask


# export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# sourceschema="work"
# sourcetable="run_202012111200_analysis_texts"
# targetschema="work"
# targettable="run_202012111200_analysis_texts_structured"
# conf="configurations/egcut_epi.ini"
# luigi --scheduler-port 8082 --module luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.analysis_data_cleaning.step01_analysis_printout_extraction.extract_analysis_structured_text ExtractStructuredText --config=$conf --role=egcut_epi_work_create --source-schema=$sourceschema --source-table=$sourcetable --target-schema=$targetschema --target-table=$targettable --workers=1 --log-level=INFO


class ExtractStructuredPrintout(CDASubtask):
    """
    Searches for analysis texts in source table's 'text' field and extract them to target table.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="work")
    source_table = luigi.Parameter(default="analysis_texts")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="analysis_texts_structured")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    def run(self):
        print("-------------------------Extract structured text---------------------------------------------")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        block_size = 10000  # how many rows we fetch from the database
        buffer_size = 1000  # how many texts we send to database at once

        cur.execute(
            sql.SQL(
                """
                    select epi_id, table_text, source_schema, source_table, source_column,
                    source_table_id, text_type, span_start, span_end from {schema}.{source} 
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
                text_type = row[6][-5:]  # parser names are wihout prefix "printout"

                if text_type == "type5":
                    continue

                type_parser = eval(text_type + "_parser")

                try:
                    result = type_parser(text, epi_id)
                except:
                    print("Exception! Could not parse table with type", text_type, "and epi_id", epi_id)

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

        # add unique id
        cur = conn.cursor()
        cur.execute(
            sql.SQL("""ALTER TABLE {schema}.{target} ADD COLUMN id SERIAL PRIMARY KEY;""").format(
                schema=sql.Identifier(self.target_schema), target=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        print("\n Total running time ", str(timedelta(seconds=time.time() - start)), "\n")
        self.log_current_time("Structure analysis printout ending time")
        tb.close()
        print("-" * 100)

        cur.close()
        conn.close()
        self.mark_as_complete()
