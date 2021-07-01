import time
from datetime import timedelta

from tqdm import tqdm
import psycopg2
from psycopg2 import sql

from cda_data_cleaning.common.table_buffer import TableBuffer
from pipelines.step01_create_training_corpus.textprocessing.text_cleaning import clean, extract_sentences


def clean_and_extract_sentences_db(db_config, source_schema, source_table, target_schema, target_table):
    """
    Takes texts ('raw_text' column) from source table.schema, cleans then separates sentences and puts the result into
     target schema.table (text column)
    :param db_config: parameters used to make a connection to the database
    for example:
    "postgresql://{username}:{password}@{host}:{port}/{database}".format(
    username="username",
    password="pass",
    host="ip",
    port=5432,
    database="dbname")
    :param source_schema: The schema, that contains the source_table
    :param source_table: The table that contains the texts which will be processed ('raw_text' column)
    :param target_schema: The schema, that contains the target_table
    :param target_table: The name of the resulting table. Must have column: 'text'
    :return:
    """
    conn = psycopg2.connect(db_config)
    cur = conn.cursor()

    block_size = 10000  # how many rows we fetch from the database
    buffer_size = 1000  # how many texts we send to database at once

    cur.execute(sql.SQL(
        "SELECT raw_text FROM {schema}.{table}"
    ).format(
        schema=sql.Identifier(source_schema),
        table=sql.Identifier(source_table),
    ))

    conn.commit()

    target_cols = [
        "text",
    ]

    tb = TableBuffer(
        conn=conn,
        schema=target_schema,
        table_name=target_table,
        buffer_size=buffer_size,
        column_names=target_cols,
        input_type="dict",
    )

    start = time.time()
    iterations = 0
    while True:
        rows = cur.fetchmany(block_size)

        if not rows:
            break

        for i, row in tqdm(enumerate(rows)):
            iterations += 1

            # Text processing
            text = row[0]
            cleaned_text = clean(text)
            result = {'text': "\n".join(extract_sentences(cleaned_text))}
            # Adding the result into the target table
            tb.append([result])
        print("Processed ", iterations, "rows, time", str(timedelta(seconds=time.time() - start)))

    # flush the remaining rows
    tb.flush()

    print("\n Total running time ", str(timedelta(seconds=time.time() - start)), "\n")
    tb.close()
    print("-" * 100)

    cur.close()
    conn.close()
