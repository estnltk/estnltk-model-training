import re
import pandas as pd
import numpy as np


class Type4Parser:
    def __init__(self):  # , parsed_table: pd.DataFrame, epi_id: str):
        # self.idx_to_remove = []  # row indicies need for deleting the rows
        # self.parsed_table = None #parsed_table
        # self.epi_id = ''#epi_id
        self.date_regex = r".*\d{2}\.\d{2}\.\d{4}"
        self.analysis_name_regex = r"[Hh]emogramm|^[Uu]riini|[Vv]ereäige|[Bb]iokeemia|[Kk]liiniline vere"
        self.values_removed_regex = r".*?Analüüs.*?"
        # self.type = None #self.find_type()

    def __call__(self, text: str, epi_id: str):
        self.epi_id = epi_id
        self.parse_type4(text)
        return self.parsed_table

    def parse_type4(self, text):
        try:
            self.parse_table(text)
            # self.type = self.find_type()
            self.add_analysis_name("analysis_name_raw")
            self.add_parameter_name_and_value("parameter_name_raw", "value_raw")
            self.add_effective_time("effective_time_raw", "value_raw")
            self.remove_useless_rows("parameter_name_raw")
            self.add_epi_id("epi_id")
            self.add_reference_values("reference_values_raw")
            self.add_parameter_unit("parameter_unit_raw")
            self.reorder_cols()
        except:
            print("Can not handle type4 epi_id", self.epi_id, ", return empty table")
            self.parsed_table = pd.DataFrame(
                columns=[
                    "epi_id",
                    "analysis_name_raw",
                    "parameter_name_raw",
                    "value_raw",
                    "parameter_unit_raw",
                    "effective_time_raw",
                    "reference_values_raw",
                    "text_raw",
                ]
            )

    def parse_table(self, text):
        text = re.sub(" +", " ", text)
        text = re.sub("\n ", "\n", text)
        self.parsed_table = pd.DataFrame(re.split(" \n", text), columns=["text_raw"])

    def add_analysis_name(self, column_name_analysis: str):
        # find indices of the rows containing analysis names in the column parameter name raw
        an_idxs_ini = self.find_idx(self.parsed_table, self.analysis_name_regex)

        if an_idxs_ini.size:
            # analysis names span over multiple columns
            an_names_str = self.parsed_table.iloc[an_idxs_ini, :].to_string(header=False, index=False)
            # make list of all the analysis names the table contains
            an_names_list = [an_names_str] if len(an_idxs_ini) == 1 else an_names_str.split("\n")

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

    def find_idx(self, df, pattern):
        # finds row indices that match with given pattern in column parameter name raw
        mask = df[["text_raw"]].apply(lambda x: x.str.contains(pattern, regex=True)).any(axis=1)
        return df[mask].index

    def find_type(self):
        contains_dash = self.parsed_table["text_raw"].str.contains(r"-", na=False)
        more_dashes, more_wo_dashes = contains_dash.value_counts() if len(contains_dash.value_counts()) == 2 else 0, 1
        if more_dashes < more_wo_dashes:
            return "4.1"  # row contains value and unit
        else:
            return "4.2"  # row contains value, reference value and unit

    def add_parameter_name_and_value(self, column_name_parameter, column_name_value):
        # print(self.type)
        self.parsed_table[[column_name_parameter, column_name_value]] = self.parsed_table.text_raw.str.split(
            " ", 1, expand=True
        )

        # TODO think way to split, when cell contains reference value, look up for more details
        # split text and number to seperate columns based on type
        # if self.type == '4.1':
        # self.parsed_table[[column_name_parameter, column_name_value]] = self.parsed_table.text_raw.str.extract('([a-zA-Z-*/%#õäöü\(\)\n ]+)([^a-zA-Z-*/%#õäöü\(\)\n ]*)', expand=True)
        # elif self.type == '4.2':
        #    # right now under value_raw is also reference value
        # self.parsed_table[[column_name_parameter, column_name_value]] = self.parsed_table.text_raw.str.split(" ", 1, expand=True)
        # gets float only after .
        # print (self.parsed_table['text'].str.findall('\d+(\.\d+)') # find integers and floats
        #                   .apply(lambda x: x if len(x) >= 1 else ['no match val'])
        #                   .str[-1])

    def add_effective_time(self, column_name_effective_time, column_name_value):
        # look for date in columns value_raw and reference_value_raw
        columns_to_search = [column_name_value]

        for col in columns_to_search:
            effective_time_raw = self.parsed_table[self.parsed_table[col].str.match(self.date_regex, na=False)][
                col
            ].to_string(index=False)

            if re.match(self.date_regex, effective_time_raw):
                # extract only the date part to effective time colun
                self.parsed_table[column_name_effective_time] = re.search(
                    "\d{2}\.\d{2}\.\d{4}", effective_time_raw
                ).group()
                return

        # effective time does not exist, add empty column
        self.parsed_table[column_name_effective_time] = None

    def add_epi_id(self, column_name_epi_id):
        self.parsed_table[column_name_epi_id] = self.epi_id

    def remove_useless_rows(self, column_name_parameter_name):
        # drop unneccesary rows
        idx_to_remove = self.parsed_table[
            self.parsed_table[column_name_parameter_name].str.contains(self.values_removed_regex, na=False)
        ].index
        self.parsed_table = self.parsed_table.drop(idx_to_remove, axis=0)

    def add_reference_values(self, col_name_reference_values):
        self.parsed_table[col_name_reference_values] = None

    def add_parameter_unit(self, col_name_parameter_unit):
        self.parsed_table[col_name_parameter_unit] = None

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


# usage
# epi_id = '123456'
# t4 = Type4Parser()
# result = t4(text, epi_id)
