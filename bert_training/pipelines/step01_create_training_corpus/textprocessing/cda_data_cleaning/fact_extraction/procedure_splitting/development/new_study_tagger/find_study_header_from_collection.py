import os
import time
import luigi
import pandas as pd
from tqdm import tqdm
from psycopg2 import sql
from estnltk import Text
from datetime import timedelta

from cda_data_cleaning.common.read_config import read_config
from cda_data_cleaning.common.db_operations import create_connection, insert_to_database
from cda_data_cleaning.common.luigi_targets import luigi_targets_folder

from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers.new_study_taggers.pricecode_tagger import (
    PriceTagger,
)
from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers.new_study_taggers.study_tagger import (
    StudyTagger,
)
from cda_data_cleaning.fact_extraction.procedure_splitting.development.taggers.new_study_taggers.study_header_tagger import (
    StudyHeaderTagger,
)


class FindStudyHeaderFromCollection(luigi.Task):
    """
    Takes the text from source table column 'text' and finds corresponding study header and price code.
    """

    prefix = luigi.Parameter()
    config_file = luigi.Parameter()
    sourceschema = luigi.Parameter()
    sourcetable = luigi.Parameter()
    targetschema = luigi.Parameter()
    targettable = luigi.Parameter()

    def requires(self):
        return []

    def run(self):
        config = read_config(str(self.config_file))
        role = config["database-configuration"]["role"]

        source_schema = self.sourceschema  # "original"
        source_table = self.sourcetable  # "procedures"

        self.prefix = str(self.prefix)
        target_schema = str(self.targetschema)  # "work"
        target_table = str(self.prefix + self.targettable)  # "run_202006101114_study"

        field = "text"
        blocksize = 10000  # how many rows to send to the database at once

        col_list = ["epi_id", "epi_type", "schema", "table_name", "field", "row_id", "study", "pricecode", "raw_text"]
        parsed_studies = pd.DataFrame(columns=col_list)

        query = sql.SQL(
            """
        -- create target table
        set role {role};
        set search_path to {target_schema};
        drop table if exists {target_table};
        create table {target_table} (
            epi_id text, 
            epi_type text, 
            schema text, 
            table_name text, 
            field text, 
            row_id int, 
            study text, 
            pricecode text, 
            raw_text text);
        reset role;

        set search_path to {source_schema};
        -- select data
        select data ->> 'text', epi_id, epi_type, id
        from {source_table};
        """.format(
                role=role,
                source_schema=source_schema,
                source_table=source_table,
                target_schema=target_schema,
                target_table=target_table,
            )
        )

        conn = create_connection(config)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()

        price_tagger = PriceTagger()
        study_tagger = StudyTagger()
        study_header_tagger = StudyHeaderTagger()

        it = 0
        start = time.time()

        while True:
            proc_rows = cur.fetchmany(blocksize)

            if not proc_rows:
                break

            # insert to database
            if len(parsed_studies) > blocksize:
                # print('Processed {} rows, running time {}'.format(it, str(timedelta(seconds=time.time() - start))))
                insert_to_database(parsed_studies, target_schema + "." + target_table, conn)
                parsed_studies = pd.DataFrame(columns=col_list)  # create new empty dataframe

            for row in tqdm(proc_rows):
                if row[0] is None:
                    continue

                text = Text(row[0])
                price_tagger.tag(text)
                study_tagger.tag(text)
                study_header_tagger.tag(text)  # requires price code and study layers

                # not a study, header and pricecode will be None
                if len(text["study_header"].spans) == 0:
                    temp_df = pd.DataFrame(
                        [[row[1], row[2], source_schema, source_table, field, row[3], None, None, row[0]]],
                        columns=col_list,
                    )
                    parsed_studies = parsed_studies.append(temp_df, ignore_index=True, sort=False)
                    continue

                # finds ALL headers in the text, so one text can become many rows in the target table
                for s in text["study_header"]:
                    study_header = s["STUDY_HEADER"] if s else None
                    pricecode = text["pricecode"][0].text if text["pricecode"] else None

                    temp_df = pd.DataFrame(
                        [
                            [
                                row[1],
                                row[2],
                                source_schema,
                                source_table,
                                field,
                                row[3],
                                study_header,
                                pricecode,
                                row[0],
                            ]
                        ],
                        columns=col_list,
                    )
                    parsed_studies = parsed_studies.append(temp_df, ignore_index=True, sort=False)
            it += 1

        insert_to_database(parsed_studies, target_schema + "." + target_table, conn)
        print("\n Total running time {} \n".format(str(timedelta(seconds=time.time() - start))))

        conn.close()
        cur.close()

        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "find_study_headers_done"))
