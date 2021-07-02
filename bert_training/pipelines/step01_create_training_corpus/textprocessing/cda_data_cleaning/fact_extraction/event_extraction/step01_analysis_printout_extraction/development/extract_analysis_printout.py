import time
import luigi
from tqdm import tqdm
from estnltk import Text
from psycopg2 import sql
from datetime import timedelta

from cda_data_cleaning.common.preprocess_text import preprocess_text
from cda_data_cleaning.common.table_buffer import TableBuffer

from cda_data_cleaning.common.luigi_tasks.cda_batch_task import CDABatchTask
from cda_data_cleaning.fact_extraction.event_extraction.analysis_printout_extraction.taggers import (
    Type1StartEndTagger,
    Type2StartEndTagger,
    Type3StartEndTagger,
    Type4StartEndTagger,
    Type5StartEndTagger,
    Type6StartEndTagger,
    Type7StartEndTagger,
    SegmentsTagger,
)

from cda_data_cleaning.fact_extraction.event_extraction.analysis_printout_extraction.texts_extraction_conf import (
    EXTRACTABLE_TABLE_FIELDS,
)

# export PYTHONPATH=~/Repos/cda-data-cleaning:$PYTHONPATH
# targettable="analysis_texts"
# prefix="run_202012251708_"
# conf="configurations/egcut_epi.ini"
# original_20181206
# luigi --scheduler-port 8082 --module cda_data_cleaning.data_cleaning.analysis_data_cleaning.analysis_printout_extraction.development.extract_analysis_text ExtractAnalysisTextDev --config=$conf --role=egcut_epi_work_create --prefix=$prefix --source-schema=original --target-schema=work --target-table=$targettable --workers=1 --log-level=INFO


class ExtractAnalysisPrintoutDev(CDABatchTask):
    """
    Searches for analysis texts in source table's 'text' field and extract them to target table.
    Source tables are specified in EXTRACTABLE_TABLE_FIELDS.
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter()
    luigi_targets_folder = luigi.Parameter("luigi_targets")  # default=".")

    def requires(self):
        sub_tasks = []
        for source_table, fields in EXTRACTABLE_TABLE_FIELDS["egcut_epi"].items():
            for field in fields:
                # having some unresolved trouble with that table
                if not (source_table == "summary" and field == "sumsum"):
                    # if not (source_table == 'anamnesis' and field == 'anamsum'):
                    continue
                sub_tasks.append(
                    ExtractTablesFromTextField(
                        config_file=self.config_file,
                        role=self.role,
                        source_schema=self.source_schema,
                        source_table=source_table,
                        target_schema=self.target_schema,
                        target_table=self.prefix + self.target_table,
                        field=field,
                        luigi_targets_folder=self.luigi_targets_folder,
                    )
                )

        return sub_tasks

    def run(self):
        print()
        self.mark_as_complete()


class ExtractTablesFromTextField(CDABatchTask):
    """
    Searches for analysis texts in source table's 'text' field and extract them to target table.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter()
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter()
    field = luigi.ListParameter()  # e.g. 'original_text'
    luigi_targets_folder = luigi.Parameter(default=".")

    def requires(self):
        return []

    def run(self):
        print("-------------------------Extract analysis from text field---------------------------------------------")
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        cur.itersize = 10000
        block_size = 100000  # 10000  # 0  # how many rows we fetch from the database
        buffer_size = 1000  # 1000  # 500 #1000 #10000 # how many texts we send to database at once

        type1_tagger = Type1StartEndTagger()
        type2_tagger = Type2StartEndTagger()
        type3_tagger = Type3StartEndTagger()
        type4_tagger = Type4StartEndTagger()
        type5_tagger = Type5StartEndTagger()
        type6_tagger = Type6StartEndTagger()
        type7_tagger = Type7StartEndTagger()
        seg_tagger = SegmentsTagger()

        tb = TableBuffer(
            conn=conn,
            schema=self.target_schema,
            table_name=self.target_table,
            buffer_size=buffer_size,
            column_names=[
                "epi_id",
                "table_text",
                "source_schema",
                "source_table",
                "source_column",
                "source_table_id",
                "text_type",
                "span_start",
                "span_end",
            ],
            input_type="dict",
        )

        print("\n\nProcessing  field ", self.field, " from table ", self.source_table)
        cur.execute(
            sql.SQL(
                """
                select id, epi_id, {field} from {schema}.{table}
                """
            ).format(
                schema=sql.Identifier(self.source_schema),
                table=sql.Identifier(self.source_table),
                field=sql.Identifier(self.field),
            )
        )

        start = time.time()
        iterations = 0

        while True:
            rows = cur.fetchmany(block_size)

            # no more rows left
            if not rows:
                break

            for row in tqdm(rows):
                iterations += 1

                # no content in text field
                if not row[2]:
                    continue

                row_id, epi_id, text = row

                text = preprocess_text(row[2])
                t = Text(text.strip())
                type1_tagger.tag(t)
                type2_tagger.tag(t)
                type3_tagger.tag(t)
                type4_tagger.tag(t)
                type5_tagger.tag(t)
                type6_tagger.tag(t)
                type7_tagger.tag(t)
                seg_tagger.tag(t)

                dictionary_list = []

                # keeping data in dictionaries is the most efficient way to send it to db later
                for table in t.table_segments.spans:
                    dictionary_data = {}

                    # do not add empty tables
                    if not table.enclosing_text:
                        continue

                    dictionary_data["source_table_id"] = row_id
                    dictionary_data["table_text"] = table.enclosing_text
                    dictionary_data["epi_id"] = epi_id
                    dictionary_data["source_schema"] = self.source_schema
                    dictionary_data["source_table"] = self.source_table
                    dictionary_data["source_column"] = self.field
                    dictionary_data["text_type"] = table.regex_type[0]
                    dictionary_data["span_start"] = table.start
                    dictionary_data["span_end"] = table.end
                    dictionary_list.append(dictionary_data)
                print(dictionary_list)
                # tb.append(dictionary_list)

            print("Processed ", iterations, "rows, time", str(timedelta(seconds=time.time() - start)))

        # flush the remaining rows
        # tb.flush()
        print("\n Total running time ", str(timedelta(seconds=time.time() - start)), "\n")
        print("-" * 100)

        tb.close()

        # self.mark_as_complete()
