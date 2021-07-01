import pandas as pd
import re


class Type1Parser:
    def __init__(self):
        self.date_regex = r".*\d{2}\.\d{2}\.\d{4}"

    def __call__(self, text: str, epi_id: str):
        self.epi_id = epi_id
        self.parse_type1(text)
        return self.parsed_table

    def parse_type1(self, text):
        try:
            self.parse_table(text, ["parameter_name_raw", "value_raw", "analysis_name_raw"])
            self.add_effective_time("effective_time_raw")  # needs to be before row removal
            self.add_analysis_name("analysis_name_raw", "value_raw")  # needs to be before row removal
            self.remove_useless_rows("value_raw")
            self.add_epi_id("epi_id")
            self.add_reference_values("reference_values_raw")
            self.add_parameter_unit("parameter_unit_raw")
            self.reorder_cols()
        # sometimes some surprise texts are classified under type1
        except:
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

    def parse_table(self, text, columns_to_extract):
        raw_df = pd.DataFrame(re.split("; |\n", text), columns=["text_raw"])
        extract_cols = pd.DataFrame(raw_df.text_raw.str.split(" ", 2).tolist(), columns=columns_to_extract)
        # keep raw value
        self.parsed_table = pd.concat((raw_df, extract_cols), axis=1)

    def add_epi_id(self, column_name_epi_id):
        self.parsed_table[column_name_epi_id] = self.epi_id

    def add_effective_time(self, column_name_effective_time):
        effective_time_raw = self.parsed_table.iloc[0, 0]
        is_date = re.match(self.date_regex, effective_time_raw)
        self.parsed_table[column_name_effective_time] = (
            re.search(self.date_regex, effective_time_raw).group(0) if is_date else None
        )

    def add_analysis_name(self, column_name_analysis, column_name_value):
        # analysis_name raw can be found in two columns: value and analysis name
        # numbers are not considered as name
        an_part1 = self.parsed_table[column_name_value][0]  # re.sub('\d', '', self.parsed_table['value_raw'][0])
        an_part2 = self.parsed_table[column_name_analysis][0]
        if an_part1 and an_part2:
            self.parsed_table[column_name_analysis] = re.sub("\d", "", an_part1) + " " + an_part2
        elif an_part1:
            self.parsed_table[column_name_analysis] = re.sub("\d", "", an_part1)
        elif an_part2:
            self.parsed_table[column_name_analysis] = re.sub("\d", "", an_part2)
        else:
            self.parsed_table[column_name_analysis] = None

        self.parsed_table[column_name_analysis] = self.parsed_table[column_name_analysis].str.capitalize()

    def remove_useless_rows(self, column_name_value):
        # drop rows without value and first row
        self.parsed_table = self.parsed_table[
            (self.parsed_table[column_name_value] != "")
            & (self.parsed_table[column_name_value].notnull())
            & (self.parsed_table[column_name_value] != ";")
        ].iloc[1:]

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
# t1 = Type1Parser()
# result = t1(text, epi_id)
