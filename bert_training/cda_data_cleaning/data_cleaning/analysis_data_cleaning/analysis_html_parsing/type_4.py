import lxml
from lxml import html
from typing import List
import pandas as pd


def get_subtype_for_type4(table: lxml.html.HtmlElement, labels: List[str]):
    """
    Determines if given html table is type 4

    :param labels: list of labels extracted from the table heading
    :param table: html table
    :returns 4 (the table type number) or False if not type 4
    """
    # example: epi_id 52746443, testcase063
    # header_cells = table.xpath('thead')[0].xpath('tr/th')
    # Check that all header cells have colspan 1
    # if (not are_simple_columns(header_cells)):
    #    print('Columns have colspan > 1')
    #    return False

    if labels == [
        "Analüüs",
        "Parameeter",
        "Referentsväärtus, otsustuspiir ja ühik",
        "Proovi võtmise aeg",
        "Tulemus",
        "Kirjeldus",
    ] or labels == [
        "Analüüsi nimetus",
        "Parameeter",
        "Referentsväärtus, otsustuspiir ja ühik",
        "Proovi võtmise aeg",
        "Tulemus",
        "Kirjeldus",
    ]:
        return 4.1
    elif labels == [
        "Analüüs",
        "Parameeter",
        "Referentsväärtus, otsustuspiir ja ühik",
        "Proovi võtmise aeg",
        "Tulemus",
        "",
    ]:
        return 4.2
    else:
        return False


def parse_type4(
    df: pd.DataFrame, labels: List[str], table_type: float, meta: List
) -> pd.DataFrame:  # meta: Dict[str, Any]
    df.columns = labels

    # if in first row 'Parameeter', 'Referentsväärtus, otsustuspiir ja ühik', 'Proovi võtmise aeg', 'Tulemus' are None then
    # first row is special and contains only analysis name so we can discard it
    if (
        df["Parameeter"][0] is None
        and df["Referentsväärtus, otsustuspiir ja ühik"][0] is None
        and df["Proovi võtmise aeg"][0] is None
        and df["Tulemus"][0] is None
    ):
        df = df.iloc[1:]

    # parameter name is located under "Analüüs", switch the values
    if table_type == 4.2:
        df.loc[:, ["Analüüs", "Parameeter"]] = df.loc[:, ["Parameeter", "Analüüs"]].values

    df.insert(loc=2, column="Ühik", value=None)  # parameter_unit is empty column

    df.columns = [
        "analysis_name_raw",
        "parameter_name_raw",
        "parameter_unit_raw",
        "reference_values_raw",
        "effective_time_raw",
        "value_raw",
        "description",
    ]

    # check if all description rows have the same values!
    if df["description"].any():
        meta.append(df["description"].iloc[0])
    df = df.drop(labels="description", axis=1)

    # analysis_substrate is empty
    df.insert(loc=6, column="analysis_substrate_raw", value=None, allow_duplicates=True)

    return df
