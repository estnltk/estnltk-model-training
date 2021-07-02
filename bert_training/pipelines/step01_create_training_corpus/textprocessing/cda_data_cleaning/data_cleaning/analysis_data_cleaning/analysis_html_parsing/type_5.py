import lxml
from lxml import html
from typing import List
import pandas as pd


def get_subtype_for_type5(table: lxml.html.HtmlElement, labels: List[str]):
    """
    Determines if given html table is type 4

    :param labels: list of labels extracted from the table heading
    :param table: html table
    :returns 5.1 or 5.2 or 5.3 (the table type number) or False if not type 5
    """

    # Check that all header cells have colspan 1
    # if (not are_simple_columns(header_cells)):
    #    print('Columns have colspan > 1')
    #    return False

    if labels == [
        "Analüüs",
        "Referentsväärtus, otsustuspiir ja ühik",
        "Proovi võtmise aeg",
        "Tulemus",
        "Tulemuse tõlgendus",
        "Kirjeldus",
    ]:
        return 5.1
    elif labels == [
        "Analüüs",
        "Parameeter",
        "Referentsväärtus, otsustuspiir ja ühik",
        "Proovi võtmise aeg",
        "Tulemus",
        "Tulemuse tõlgendus",
        "Kirjeldus",
    ]:
        return 5.2
    elif labels == [
        "Analüüs",
        "Parameeter",
        "Referentsväärtus, otsustuspiir ja ühik",
        "Proovi võtmise aeg",
        "Tulemus",
        "Tulemuse tõlgendus",
        "Soetud tulemus",
    ]:
        return 5.3
    else:
        return False


def parse_type5(
    df: pd.DataFrame, labels: List[str], parse_type: float, meta: List
) -> pd.DataFrame:  # meta: Dict[str, Any]

    if parse_type == 5.1:
        df.insert(loc=0, column="analysis_name", value=None)  # analysis_name is empty column

    df.insert(loc=2, column="parameter_unit_raw", value=None)  # parameter_unit is empty column

    df.columns = [
        "analysis_name_raw",
        "parameter_name_raw",
        "parameter_unit_raw",
        "reference_values_raw",
        "effective_time_raw",
        "value_raw",
        "description1",
        "description2",
    ]

    # add all non null values as meta from columns 'Tulemuse tõlgendus', 'Kirjeldus'
    meta.append((df[["description1", "description2"]].T.stack().values).tolist())

    df = df.drop(labels=["description1", "description2"], axis=1)

    # analysis_substrate is empty
    df.insert(loc=6, column="analysis_substrate_raw", value=None, allow_duplicates=True)
    return df
