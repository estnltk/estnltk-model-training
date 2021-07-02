import pandas as pd
import re
import numpy as np

# TODO
# for some tables one row is on multiple rows,
# this information gets lost because we assume that every analysis is on seperate row
# e.g. epi_id = '14210942'
# Hb,     ,         ,   ,
# 00:00:00, 00:00:00,   ,
# 61 ↓↓,    94 ↓,       ,
# 101,      113,     110, g/L


class Type3Parser:
    def __init__(self):
        self.analysis_name_regex = r"[Hh]emogramm|^[Uu]riini|[Vv]ereäige"

    def __call__(self, text: str, epi_id: str):
        self.epi_id = epi_id
        self.parse_type3(text)
        return self.parsed_table

    def parse_type3(self, text):
        self.parse_table(text)
        self.add_header("parameter_name_raw", "parameter_unit_raw")
        self.add_analysis_name(
            "analysis_name_raw", "parameter_name_raw"
        )  # needs to be done before add_effective_time_and_value, otherwise it multiplies
        self.add_effective_time_and_value(
            "parameter_name_raw",
            "parameter_unit_raw",
            "analysis_name_raw",
            "effective_time_raw",
            "value_raw",
            "text_raw",
        )
        self.remove_useless_rows("value_raw", "parameter_name_raw")
        self.add_epi_id("epi_id")
        self.add_reference_values("reference_values_raw")
        self.reorder_cols()

    def parse_table(self, text):
        text = re.sub(" +", " ", text)
        # number of columns is different in each table
        raw_df = pd.DataFrame(re.split("\n", text), columns=["text_raw"])
        raw_df = raw_df[raw_df["text_raw"].str.strip() != ""].reset_index(drop=True)
        col_count = self.find_col_count(raw_df) + 2
        extract_cols = pd.DataFrame(raw_df.text_raw.str.strip().str.split("|", col_count).tolist())

        # check if the whole table is empty OR
        # if first row contains only one '|', then it is actually NOT analysis type3 text
        all_cells_empty = extract_cols.eq("").all(axis=None)
        if all_cells_empty or str(raw_df.iloc[0, :]).count("|") == 1:
            self.parsed_table = pd.DataFrame(columns=["text_raw", "parameter_name_raw", "parameter_unit_raw"])
        else:
            self.parsed_table = pd.concat((raw_df, extract_cols), axis=1)

    def find_col_count(self, df):
        # last row is header
        if "Mõõtühik" in df.iloc[-1, 0]:
            header_row = -1
        # first row is header
        else:  ## if self.parsed_table.iloc[0,0].startswith('Uuring'):
            header_row = 0
        return df["text_raw"].iloc[header_row].count("|")

    def add_header(self, column_name_parameter_name, column_name_parameter_unit):

        try:
            # check if first/last row is header
            first_is_header = True if "Mõõtühik" in self.parsed_table.iloc[0, 0] else False
            last_is_header = True if "Mõõtühik" in self.parsed_table.iloc[-1, 0] else False

            # add first/last row as header (Uuring, date_1, ..., date_n, Mõõtühik)
            if first_is_header:
                col_names = ["text_raw"] + self.parsed_table.iloc[0].tolist()[1:]
                self.parsed_table.columns = [col.strip() for col in col_names]
                self.parsed_table = self.parsed_table.iloc[1:]
            elif last_is_header:
                col_names = ["text_raw"] + self.parsed_table.iloc[-1].tolist()[1:]
                self.parsed_table.columns = [col.strip() for col in col_names]
                self.parsed_table = self.parsed_table.iloc[:-1]
            # no header
            else:
                n = len(self.parsed_table.columns)
                # need some placeholder (NaN) for effective_time column, otherwise those rows will be deleted
                self.parsed_table.columns = ["text_raw", "", "Uuring"] + ["NaN"] * (n - 5) + ["Mõõtühik", ""]

            self.parsed_table.reset_index(drop=True, inplace=True)
            self.parsed_table.columns.values[2] = column_name_parameter_name
            self.parsed_table = self.parsed_table.rename(
                columns={"Mõõtühik": column_name_parameter_unit, "mõõtühik": column_name_parameter_unit}
            )
        # sometimes non-type3 texts end up here and therefore the assignment of column names does not work
        except:
            self.parsed_table = pd.DataFrame(columns=["text_raw", "parameter_name_raw", "parameter_unit_raw"])

    def add_epi_id(self, column_name_epi_id):
        self.parsed_table[column_name_epi_id] = self.epi_id

    def add_effective_time_and_value(
        self,
        column_name_parameter_name,
        column_name_parameter_unit,
        column_name_analysis_name,
        column_name_effective_time,
        column_name_value,
        column_name_text_raw,
    ):
        # convert from wide format to long
        # because initially effective_times are columns, changes them to rows
        self.parsed_table = self.parsed_table.melt(
            id_vars=[
                column_name_parameter_name,
                column_name_parameter_unit,
                column_name_analysis_name,
                column_name_text_raw,
            ],
            var_name=column_name_effective_time,
            value_name=column_name_value,
        )
        # remove empty rows
        self.parsed_table = self.parsed_table[
            self.parsed_table[column_name_effective_time].str.strip() != ""
        ]  # self.parsed_table['effective_time_raw'] != '']

    def find_idx(self, df, pattern, column_name):
        # finds row indices that match with given pattern in column parameter name raw
        mask = df[[column_name]].apply(lambda x: x.str.contains(pattern, regex=True)).any(axis=1)
        return df[mask].index

    def add_analysis_name(self, column_name_analysis, column_name_parameter_name):
        # find indices of the rows containing analysis names in the column parameter name raw
        an_idxs_ini = self.find_idx(self.parsed_table, self.analysis_name_regex, column_name_parameter_name)

        if an_idxs_ini.size:
            an_names_list = self.parsed_table.iloc[an_idxs_ini][column_name_parameter_name].tolist()
            an_idxs = np.append(
                an_idxs_ini, len(self.parsed_table) - 1
            )  # adding as the last index the last row of table

            # assigne corresponding analysis names to each row
            for i in range(len(an_names_list)):
                self.parsed_table.loc[an_idxs[i] : an_idxs[i + 1], column_name_analysis] = an_names_list[i]

        # no analysis_names
        else:
            self.parsed_table[column_name_analysis] = None  # add empty column

        # remove analysis_name row otherwise it multiplies during convversion from wide to long
        self.parsed_table = self.parsed_table.drop(an_idxs_ini, axis=0)
        self.parsed_table[column_name_analysis] = self.parsed_table[column_name_analysis].str.capitalize()

    def remove_useless_rows(self, column_name_value, column_name_parameter_name):
        if not self.parsed_table.empty:
            self.parsed_table = self.parsed_table[
                (self.parsed_table[column_name_value].str.strip() != "")
                & (~self.parsed_table[column_name_value].isnull())
                & (~self.parsed_table[column_name_parameter_name].str.contains("ANONYM.*?>$"))
            ]

    def add_reference_values(self, col_name_reference_values):
        self.parsed_table[col_name_reference_values] = None

    def reorder_cols(self):
        columns = [
            "epi_id",
            "analysis_name_raw",
            "parameter_name_raw",
            "value_raw",
            "parameter_unit_raw",
            "effective_time_raw",
            "reference_values_raw",
            "text_raw",
        ]
        self.parsed_table = self.parsed_table.reindex(columns=columns)


# text = '''|Uuring|04.11|05.11|Mõõtühik|
# |Bilirubiin (konjugeeritud) se...|3.2| |µmol/L|
# |AB0-veregrupi ja Rh(D) kinnita...| | | |
# |AB0 veregrupp|0| | |
# |RhD veregrupp|Positiivne| | |
# |Alaniini aminotransferaas seer...|22| |U/L| '''
# epi_id = '123456'
# t3 = Type3Parser()
# result = t3(text, epi_id)
# print(result)
