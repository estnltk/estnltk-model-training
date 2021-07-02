import lxml
from lxml import html
from typing import List
import pandas as pd
from copy import copy
from cda_data_cleaning.common.html_parsing.table_parsing import is_date


def get_subtype_for_type3(table: lxml.html.HtmlElement, labels: List[str]):
    """
    Determines if given html table is type 3

    :param  labels: list of labels extracted from the table heading
    :param table: html table
    :returns 3 (the table type number) or False if not type 3
    """
    # import simple columns: from cda_data_cleaning.common.html_parsing.table_parsing import are_simple_columns,
    # header_cells = table.xpath('thead')[0].xpath('tr/th')
    # Check that all header cells have colspan 1
    # if (not are_simple_columns(header_cells)):
    #    print('Columns have colspan > 1')
    #    return False

    # Check that the first column name in the allowed_analysis_names list
    # Check that the second column name in a header is "Rerentsväärtus"
    # Check that all other (non-empty) column names are dates
    # Important: in allowed_analysis_names, each name must start with capital letter and all others should be small
    allowed_analysis_names = [
        "Seroloogiline analüüs",
        "Uriini analüüs",
        "Mikrobioloogia analüüs",
        "Kliiniline vereanalüüs",
        "Immuunmeetoditel uuringud",
        "Hemostasiogramm",
        "Happe-alus tasakaal",
        "Biokeemia analüüs",
        "Kliiniline vere ja glükohemoglobiini analüüs",
        "Kl keemia ja immunoloogia automaatliinil teostatud uuring",
        "Liikvori analüüs",
        "Väljaheite analüüs",
        "Rooja uuringud",
        "Mikrobioloogia pcr meetodil",
        "Punktaadi analüüs",
        "Tbc mikrobioloogia analüüs",
        "Allergia testid",
        "Väljast tellitud analüüsid",
        "Elektroforeetiline uuring",
        "Toksikoloogiline analüüs",
        "Act test",
        "Naha- ja suguhaiguste analüüs",
        "Inr määramine",
        "Automaatliinil teostatud uuring",
        "Molekulaaruuringud",
        "Tromboelastogramm",
        "Toksikoloogia analüüs hiiul",
        "Metallide uuringud",
        "Läbivoolutsütomeetria",
    ]

    if (
        labels[0] in allowed_analysis_names
        and not is_date(labels[0])
        and (labels[1] == "Referentsväärtus" or labels[1] == "Rerentsväärtus")
    ):
        for i in range(2, len(labels)):
            if labels[i]:  # For example epicrisis 26992474 has label ''
                if not is_date(labels[i]):
                    return False
        return 3
    else:
        return False


def parse_type3(df: pd.DataFrame, labels: List[str], table_type: float, meta: List) -> pd.DataFrame:
    df.columns = labels

    if df.iloc[0, 0] == "Materjal":  # first row is special, potentitally containing substrate
        substrate = df.iloc[0, -1]
        df = copy(df.iloc[1:])  # we can now discard the first row as it only contained information about substrate
    else:
        substrate = None

    analysis_name = labels[0]  # analysis_name is the first column name in header
    df.insert(loc=0, column="Analüüs", value=analysis_name)
    df.rename(
        columns={analysis_name: "Parameeter"}, inplace=True
    )  # under the analysis_name column values are actually parameter names

    not_date_columns = ["Analüüs", "Parameeter", "Rerentsväärtus"]
    date_columns = [x for x in list(df.columns) if x not in not_date_columns]
    df = df.melt(id_vars=not_date_columns, value_vars=date_columns)  # wide to long

    df.insert(loc=2, column="Ühik", value=None)  # parameter_unit is empty column

    df["Substraat"] = substrate

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
