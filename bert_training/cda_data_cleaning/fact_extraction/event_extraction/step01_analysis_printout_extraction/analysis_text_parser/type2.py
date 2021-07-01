import re
from typing import List
import numpy as np
import pandas as pd


class Type2Parser:
    def __init__(self):
        self.idx_to_remove = []
        self.date_regex = r".*\d{2}\.\d{2}\.\d{4}"
        self.analysis_name_regex = r"[Hh]emogramm|^[Uu]riini|[Vv]ereäige"
        self.values_removed_regex = (
            r"^$|MATERJAL|VASTUSED|ANALÜÜSID|Märkus|V\d{12}|YY\d{8,10}|IP\d{10}|" + self.date_regex
        )

    def __call__(self, text: str, epi_id: str):
        self.epi_id = epi_id
        self.parse_type2(text)
        return self.parsed_table

    def parse_type2(self, text):
        self.parse_table(text, ["parameter_name_raw", "value_raw", "reference_values_raw"])
        self.add_analysis_names("analysis_name_raw", "parameter_name_raw")
        self.add_effective_time("effective_time_raw", "value_raw", "reference_values_raw")
        self.remove_useless_rows("parameter_name_raw")
        self.add_epi_id("epi_id")
        self.add_parameter_unit("parameter_unit_raw")
        self.reorder_cols()

    def parse_table(self, text, column_names_to_extract):
        # different row seperators

        # different row seperators
        if "&lt;br /&gt;" in text:
            raw_df = pd.DataFrame(re.split(r"&lt;br /&gt;", text), columns=["text_raw"])
        else:
            raw_df = pd.DataFrame(text.split("\n"), columns=["text_raw"])

        # some wrong texts, that do not contain ' ' can end up in the parser, trying to split them would cause error
        try:
            extract_cols = pd.DataFrame(raw_df.text_raw.str.split(" ", 2).tolist(), columns=column_names_to_extract)
            self.parsed_table = pd.concat((raw_df, extract_cols), axis=1)
        except:
            self.parsed_table = pd.DataFrame(columns=column_names_to_extract)

    def find_idx(self, df, pattern, column_name):
        # finds row indices that match with given pattern in column parameter name raw
        mask = df[[column_name]].apply(lambda x: x.str.contains(pattern, regex=True)).any(axis=1)
        return df[mask].index

    def add_analysis_names(self, column_name_analysis, column_name_parameter_name):
        # find indices of the rows containing analysis names in the column parameter name raw
        an_idxs_ini = self.find_idx(self.parsed_table, self.analysis_name_regex, column_name_parameter_name)
        self.idx_to_remove = an_idxs_ini  # removes those rows later

        if an_idxs_ini.size:
            # analysis names span over multiple columns
            an_names_str = self.parsed_table.iloc[an_idxs_ini, 1:].to_string(
                header=False, index=False
            )  # 1: because we do not want to include 'text_raw' values
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

        self.parsed_table[column_name_analysis] = self.parsed_table[column_name_analysis].str.capitalize()

    def add_effective_time(self, column_name_effective_time, column_name_value, column_name_reference):
        # look for date in columns value_raw and reference_value_raw
        columns_to_search = [column_name_value, column_name_reference]

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
        # e.g. MATERJAL, V100249522021, YY00913147, IP0004213344, etc
        idx_to_remove2 = self.parsed_table[
            self.parsed_table[column_name_parameter_name].str.match(self.values_removed_regex, na=False)
        ].index
        self.parsed_table = self.parsed_table.drop(np.append(self.idx_to_remove, idx_to_remove2), axis=0)

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


# text = '''ANALÜÜSIDE TELLIMUS nr: 1560486
#
# MATERJAL:
# V100249522021 20.07.2009 09:44 (võetud: 20.07.2009 00:00)
#
# VASTUSED:
# Hemogramm
# WBC 7.56 (3,5 .. 8,8 E9/L )
# RBC 4.13 (3,9 .. 5,2 E12/L )
# HGB 131 (117 .. 153 g/L )
# HCT 39 (35 .. 46 % )
# MCV 93.5 (82 .. 98 fL )
# MCH 31.7 (27 .. 33 pg )
# MCHC 339 (317 .. 357 g/L )
# PLT 219 (145 .. 390 E9/L )
# RDW-CV 14.0 (11,6 .. 14 % ) '''
# epi_id = '123'
# t2 = Type2Parser()
# result = t2(text, epi_id)
# print(result)
