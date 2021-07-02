import os
import re
import luigi

from psycopg2 import sql
from estnltk import Text
from tqdm import tqdm

from cda_data_cleaning.common.read_config import read_config
from cda_data_cleaning.common.luigi_targets import luigi_targets_folder
from cda_data_cleaning.common.db_operations import create_connection

from .drug_extraction import ExtractPrecise4qDrugEntries


class CleanPrecise4qDrugEntries(luigi.Task):
    """
    Cleans drug entries defined in Precise4q study using various simplifications.
    """

    prefix = luigi.Parameter(default="")
    config_file = luigi.Parameter()

    def requires(self):
        return [ExtractPrecise4qDrugEntries(self.prefix, self.config_file)]

    def run(self):
        config = read_config(str(self.config_file))
        role = sql.Identifier(config["database-configuration"]["role"])
        schema = sql.Identifier(config["database-configuration"]["work_schema"])
        table = sql.Identifier(self.prefix + "_drug_free_text" if len(self.prefix) else "drug_free_text")

        print("--------------------------Cleaning Precise4q drug entries--------------------------------------------")

        conn = create_connection(config)
        cur = conn.cursor()
        print(
            "* Cleaning table {}".format(
                sql.SQL("{schema}.{table}").format(schema=schema, table=table).as_string(conn)
            )
        )

        print("* Removing 4,2MG/ML")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = regexp_replace(drug, '[0-9]+(,[0-9]+)?\s*(mg|MG)/[0-9]*\s*(ml|ML)', '', 'g')
            WHERE drug_normalised = '' AND drug ~ '[0-9]+(,[0-9]+)?\s*(mg|MG)/[0-9]*\s*(ml|ML)';
            """
            ).format(role=role, schema=schema, table=table)
        )

        print("* Removing  4MG/5,2MG/G")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = regexp_replace(drug, '[0-9]+\s*(mg|MG)/[0-9]+(,[0-9]+)?\s*(mg|MG)(/G)?', '', 'g')
            WHERE drug_normalised = '' AND drug ~ '[0-9]+\s*(mg|MG)/[0-9]+(,[0-9]+)?\s*(mg|MG)(/G)?';
            """
            ).format(role=role, schema=schema, table=table)
        )

        print("* Removing 1,3 MG")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = regexp_replace(drug, '[0-9]+(,[0-9]+)?\s*(g|mg|MG|MCG)', '', 'g')
            WHERE drug_normalised = '' AND drug ~ '[0-9]+(,[0-9]+)?\s*(g|mg|MG|MCG)';
            """
            ).format(role=role, schema=schema, table=table)
        )

        print("* Removing numbers, times and percents at the end")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = regexp_replace(drug, '[0-9]+(,[0-9]+)?(\s*%)?$', '', 'g')
            WHERE drug_normalised = '' AND drug ~ '[0-9]+(,[0-9]+)?(\s*%)?$';
            """
            ).format(role=role, schema=schema, table=table)
        )

        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = regexp_replace(drug, '[0-9]+x$', '', 'g')
            WHERE drug_normalised = '' AND drug ~ '[0-9]+x$';
            """
            ).format(role=role, schema=schema, table=table)
        )

        print("* Adding names without problems")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = drug
            WHERE drug_normalised = '';
            """
            ).format(role=role, schema=schema, table=table)
        )

        print("* Removing trash from beginning")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = regexp_replace(drug_normalised, '^(\.|,|\(|\)|>|/|-|\+|;|:|\s)+', '', 'g')
            WHERE drug_normalised ~ '^(\.|,|\(|\)|>|/|-|\+|;|:|\s)+';
            """
            ).format(role=role, schema=schema, table=table)
        )

        print("* Removing trash from the end")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = regexp_replace(drug_normalised, '(\.|,|\(|\)|-|\+|;|:|`|´|\s)+$', '', 'g')
            WHERE drug_normalised ~ '(\.|,|\(|\)|-|\+|;|:|`|´|\s)+$';
            """
            ).format(role=role, schema=schema, table=table)
        )

        print("* Unifing whitespace usage")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = regexp_replace(drug_normalised, '\s+', ' ', 'g')
            WHERE drug_normalised ~ '\s+';
            """
            ).format(role=role, schema=schema, table=table)
        )

        print("* Convert names to lower case")
        cur.execute(
            sql.SQL(
                """
            SET ROLE {role};
            UPDATE {schema}.{table}
            SET drug_normalised = lower(drug_normalised)
            WHERE drug_normalised ~ E'^[[:upper:]]';
            """
            ).format(role=role, schema=schema, table=table)
        )

        conn.commit()
        conn.close()

        print("* Cleaning complete")
        os.makedirs(luigi_targets_folder(self.prefix, self.config_file, self), exist_ok=True)
        with self.output().open("w"):
            pass

    def output(self):
        folder = luigi_targets_folder(self.prefix, self.config_file, self)
        return luigi.LocalTarget(os.path.join(folder, "clean_precise4q_drugs"))
