import re
import time
import luigi
from datetime import timedelta
from psycopg2 import sql
from estnltk import Text
from tqdm import tqdm

from cda_data_cleaning.common.table_buffer import TableBuffer
from cda_data_cleaning.common.luigi_tasks import CDASubtask

from .taggers.drug_tagger import DrugTagger
from .taggers.drug_grammar_token_tagger import DrugGrammarTokenTagger
from .taggers.drug_tagger_type2 import DrugTaggerType2


class ParseDrugTextField(CDASubtask):
    """
    Takes the text from column 'text' in table 'original.drug' and parses all the useful information to a target table.
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter()
    source_schema = luigi.Parameter(default="original")
    source_table = luigi.Parameter(default="drug")
    target_schema = luigi.Parameter(default="work")
    target_table = luigi.Parameter(default="drug_parsed")
    luigi_targets_folder = luigi.Parameter(default=".")
    requirement = luigi.TaskParameter()

    source_tables = ["drug"]
    target_tables = ["drug_parsed"]

    def run(self):
        self.log_current_action("Step01 Parsed drug text")
        self.log_dependencies()
        conn = self.create_postgres_connection(self.config_file)
        cur = conn.cursor()

        # create table for parsed informtion from drug table text field
        cur.execute(
            sql.SQL(
                """
            set role {role};
            drop table if exists {schema}.{target_table};
            create table {schema}.{target_table} 
            (
                id bigint, 
                epi_id text,
                epi_type text, 
                recipe_code text, 
                date text, 
                atc_code text,
                drug_name text, 
                active_ingredient text, 
                dose_quantity text,
                dose_quantity_unit text, 
                rate_quantity text, 
                rate_quantity_unit text,
                drug_form_administration_unit_code_display_name text, 
                package_size text,
                text_type float
            );
            reset role;
            """
            ).format(
                role=sql.Identifier(self.role),
                schema=sql.Identifier(self.target_schema),
                target_table=sql.Identifier(self.target_table),
            )
        )
        conn.commit()

        # Create pandas dataframe where to store intermediate results
        col_list = [
            "id",
            "epi_id",
            "epi_type",
            "recipe_code",
            "date",
            "atc_code",
            "drug_name",
            "active_ingredient",
            "dose_quantity",
            "dose_quantity_unit",
            "rate_quantity",
            "rate_quantity_unit",
            "drug_form_administration_unit_code_display_name",
            "package_size",
            "text_type",
        ]

        iterations = 0
        buffer_size = 10000
        block_size = 10000

        # used for sending parsed data to database
        tb = TableBuffer(
            conn=conn,
            schema=self.target_schema,
            table_name=str(self.target_table),
            buffer_size=buffer_size,
            column_names=col_list,
            input_type="dict",
        )

        cur.execute(
            sql.SQL(
                """
            select * from {schema}.{source_table};
            """
            ).format(schema=sql.Identifier(self.source_schema), source_table=sql.Identifier(self.source_table))
        )

        # Load taggers for parsing texts
        token_tagger = DrugGrammarTokenTagger()
        drug_tagger = DrugTagger()
        workaround_drug_tagger = DrugTaggerType2()

        start = time.time()
        while True:
            # fetch the number of rows given in the block_size
            drug_rows = cur.fetchmany(block_size)

            # no more rows left in the data table
            if not drug_rows:
                break

            # Parse the data row by row. Ignore empty rows
            for i, row in tqdm(enumerate(drug_rows)):
                if not row[3]:
                    continue

                # Workaround to make parsing long texts quicker
                # Uses separate grammar for constructions that are slow to parse
                # Example of a slow text:
                # Väljastatud ravimite ATC koodid: H02AB04 H02AB04 A02BC01 C09CA07 ... H02AB04

                regex = "^Väljastatud ravimite ATC koodid:"
                if re.match(regex, row[3]):
                    t = Text(row[3])
                    token_tagger.tag(t)
                    workaround_drug_tagger.tag(t)
                # Not a slow Type 2 row
                else:
                    # parse rows
                    t = Text(row[3])
                    token_tagger.tag(t)
                    drug_tagger.tag(t)

                for j in t.parsed_drug:
                    date = ""
                    atc_code = ""
                    drug_name = ""
                    active_ingredient = ""
                    dose_quantity = ""
                    dose_quantity_unit = ""
                    rate_quantity = ""
                    rate_quantity_unit = ""
                    recipe_code = ""
                    package_size = ""
                    drug_form_administration_unit_code_display_name = ""
                    text_type = ""
                    if j.date:
                        date = j.date
                    if j.atc_code:
                        atc_code = j.atc_code
                    if j.drug_name:
                        drug_name = j.drug_name
                    if j.active_ingredient:
                        active_ingredient = j.active_ingredient
                    if j.dose_quantity:
                        dose_quantity = j.dose_quantity
                    if j.dose_quantity_unit:
                        dose_quantity_unit = j.dose_quantity_unit
                    if j.rate_quantity:
                        rate_quantity = j.rate_quantity
                    if j.rate_quantity_unit:
                        rate_quantity_unit = j.rate_quantity_unit
                    if j.recipe_code:
                        recipe_code = j.recipe_code
                    if j.package_size:
                        package_size = j.package_size
                    if j.package_type:
                        drug_form_administration_unit_code_display_name = j.package_type
                    if j.text_type:
                        text_type = j.text_type

                    temp_dict = {
                        "id": row[0],
                        "epi_id": row[1],
                        "epi_type": row[2],
                        "recipe_code": recipe_code,
                        "date": date,
                        "atc_code": atc_code,
                        "drug_name": drug_name,
                        "active_ingredient": active_ingredient,
                        "dose_quantity": dose_quantity,
                        "dose_quantity_unit": dose_quantity_unit,
                        "rate_quantity": rate_quantity,
                        "rate_quantity_unit": rate_quantity_unit,
                        "drug_form_administration_unit_code_display_name": drug_form_administration_unit_code_display_name,
                        "package_size": package_size,
                        "text_type": text_type,
                    }

                    tb.append([temp_dict])

            iterations += 1
            # print the current state
            print(
                "Processed ",
                iterations * block_size,
                " rows, running time ",
                str(timedelta(seconds=time.time() - start)),
            )

        tb.flush()
        tb.close()
        print("\n Total running time ", str(timedelta(seconds=time.time() - start)), "\n")

        cur.close()
        conn.close()
        self.mark_as_complete()
