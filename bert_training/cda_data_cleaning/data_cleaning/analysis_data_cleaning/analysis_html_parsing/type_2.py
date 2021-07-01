import lxml
from lxml import html
from typing import List
import pandas as pd


def get_subtype_for_type2(table: lxml.html.HtmlElement, labels: List[str]):
    # Check that header is one of the following
    if labels == ["Analüüs", "Parameeter", "Ühik", "Kuupäev", "Tulemus"]:  # 17571635
        return 2.1
    elif labels == ["Analüüs", "Parameeter", "Ühik", "Referentsväärtus", "Tulemus", "Kuupäev"]:  # 146543318
        return 2.2
    elif labels == ["Analüüs", "Parameeter", "Ühik", "Referentsväärtus", "Kuupäev", "Tulemus"]:  # 82763728, 41090388
        return 2.3
    elif labels == ["Analüüs", "Ühik", "Referentsväärtus", "Kuupäev", "Tulemus"]:  # 53015838, testcase020
        return 2.4
    else:
        return False


def parse_type2(df: pd.DataFrame, labels: List[str], table_type: float, meta: List) -> pd.DataFrame:
    # in normacl case, number of body columns matches the number of header column names
    # if not, then it is special case of 2.3 meaning there is one more column is needed and
    # values of Kuupäev and Referentsväärtus should be shifted right
    if len(df.columns) == len(labels):
        df.columns = labels

    if table_type == 2.1:  # missing column "reference value"
        df.insert(loc=3, column="Referentsväärtus", value=None, allow_duplicates=True)

    # switch columns 'Kuupäev' and 'Tulemus'
    elif table_type == 2.2:
        col_list = list(df)
        col_list[4], col_list[5] = col_list[5], col_list[4]
        df = df.ix[:, col_list]

    # 2.3 is almost always already in right format
    # but in some cases Kuupäev is under Referentsväärtus and Tulemus is under Kuupäev
    #   (when in meta it says 'Less columns in body than in header')
    # example epi_id = 10212755
    # Analüüs                    Parameeter                                                 Ühik  Referentsväärtus  Kuupäev    Tulemus
    # Mikrobioloogilised uuringud Cf - Aer(Aeroobse mikrofloora  külv(emakakaelaeritisest))        21.10.2010       Negatiivne
    # then we need to shift those two columns one to right

    elif table_type == 2.3 and meta != [] and meta[0] == "Less columns in body than in header":
        df.insert(loc=5, column="Tulemus", value=None, allow_duplicates=True)
        df.columns = labels
        # df['Y'].fillna(df['X'])
        df2 = df.copy()
        df2.loc["Tulemus"] = df["Kuupäev"]
        df2.loc["Kuupäev"] = df["Referentsväärtus"]
        df2.loc["Referentsväärtus"] = None
        # df = df.copy()
        # df['Tulemus'] = df['Kuupäev']
        # df['Kuupäev'] = df['Referentsväärtus']
        # df['Referentsväärtus'] = None

    elif table_type == 2.4:
        allowed_analysis_names = "[Hh]ematoloogilised ja uriini uuringud|[Uu]riini sademe mikroskoopia|[Hh]emogramm|[Uu]riini ribaanalüüs|[Ss]eroloogiline analüüs|[Uu]riini analüüs|[Mm]ikrobioloogia analüüs|[Kk]liiniline vereanalüüs|[Ii]mmuunmeetoditel uuringud|[Hh]emostasiogramm|[Hh]appe-alus tasakaal|[Bb]iokeemia analüüs|[Kk]liiniline vere ja glükohemoglobiini analüüs|[Kk]l keemia ja immunoloogia automaatliinil teostatud uuring|[Ll]iikvori analüüs|[Vv]äljaheite analüüs|[Rr]ooja uuringud|[Mm]ikrobioloogia pcr meetodil|[Pp]unktaadi analüüs|[Tt]bc mikrobioloogia analüüs|[Aa]llergia testid|[Vv]äljast tellitud analüüsid|[Ee]lektroforeetiline uuring|[Tt]oksikoloogiline analüüs|[Aa]ct test|[Nn]aha- ja suguhaiguste analüüs|[Ii]nr määramine"

        # finding the indices of rows where instead of parameter names are analysis names
        an_indices = df[df["Analüüs"].str.contains(allowed_analysis_names)].index.tolist()
        an_names = df[df["Analüüs"].str.contains(allowed_analysis_names)]["Analüüs"].tolist()

        # adding column where real/new analysis names will be stored
        df.insert(loc=0, column="Analüüs_new", value=None, allow_duplicates=True)

        # adding analysis name to each of the parameter name that follows the analysis name
        for i, (idx, name) in enumerate(zip(an_indices, an_names)):
            idx_start = idx
            # for last analysis name is the ending index the end of the table
            # for others is the ending index when new analysis name starts
            idx_end = len(df) - 1 if i == len(an_indices) - 1 else an_indices[i + 1]
            df.loc[idx_start:idx_end, "Analüüs_new"] = name

    # analysis_substrate is empty
    df.insert(loc=6, column="analysis_substrate", value=None, allow_duplicates=True)
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
