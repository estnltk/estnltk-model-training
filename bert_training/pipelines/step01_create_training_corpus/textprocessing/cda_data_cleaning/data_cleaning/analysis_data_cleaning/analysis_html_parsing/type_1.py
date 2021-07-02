import lxml
from lxml import html
from typing import List
import pandas as pd
from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.common.html_parsing.table_parsing import is_date


def get_subtype_for_type1(table: lxml.html.HtmlElement, labels: List[str]):
    """
    Determines if given html table is type 1

    :param labels: list of labels extracted from the table heading
    :param table: html table
    :returns 1 (the table type number) or False if not type 1
    """

    # header_cells = table.xpath('thead')[0].xpath('tr/th')
    # import simple columns: from cda_data_cleaning.common.html_parsing.table_parsing import are_simple_columns,
    # print('simple?', are_simple_columns(header_cells))
    # Check that all header cells have colspan 1
    # if (not are_simple_columns(header_cells)):
    #    print('Columns have colspan > 1')
    #    return False

    # Check that there are columns "Analüüsi nimetus", "Parameeter", "Referents", "Ühik"
    # Check that all other columns are dates
    # For example epicrisis 126337634 (three date columns), 185784708 (one date column), 63672319 (no date columns)

    date_columns = []
    not_date_columns = []
    for label in labels:
        if is_date(label):
            date_columns.append(label)
        else:
            not_date_columns.append(label)

    allowed_not_date_columns1 = ["Analüüsi nimetus", "Parameeter", "Referents", "Ühik"]
    allowed_not_date_columns2 = ["Anal??si nimetus", "Parameeter", "Referents", "?hik"]

    if not_date_columns == allowed_not_date_columns1 or not_date_columns == allowed_not_date_columns2:
        return 1
    else:
        return False


def parse_type1(df: pd.DataFrame, labels: List[str], table_type: float, meta: List) -> pd.DataFrame:
    """
    Parses pandas DataFrame that is from type 1 to canoncial form

    :param labels:
    :param table_type:
    :param meta:
    :param df: pandas dataFrame from parsed html body,
           labels: list of labels extracted from the table heading,
           table_type: number of the table parse type
    :returns long dataframe with appropriate column names
    """

    df.columns = labels

    if "?" in labels[0]:
        not_date_columns = ["Anal??si nimetus", "Parameeter", "?hik", "Referents"]
    else:
        not_date_columns = ["Analüüsi nimetus", "Parameeter", "Ühik", "Referents"]
    date_columns = [x for x in labels if x not in not_date_columns]

    # date columns to rows (wide to long)
    df = df.melt(id_vars=not_date_columns, value_vars=date_columns)

    # analysis_substrate is empty
    df.insert(loc=6, column="analysis_substrate_raw", value=None, allow_duplicates=True)
    df.columns = [
        "analysis_name_raw",
        "parameter_name_raw",
        "parameter_unit_raw",
        "reference_values_raw",
        "effective_time_raw",
        "value_raw",
        "analysis_substrate_raw",
    ]
    return df
