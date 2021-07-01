import re
import pandas as pd


class Type7Parser:
    def __init__(self):
        self.inside_paren_regex = r"\((.*?)\)"
        self.date_regex = r"((0[1-9]|[12][0-9]|3[01]|[0-9])\.\d{2}\.\d{4})"
        self.date_time_regex = r"\d{1,2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}:"

    def __call__(self, text: str, epi_id: str):
        self.epi_id = epi_id
        self.parse_type7(text)
        return self.parsed_table

    def parse_type7(self, text: str):
        self.parse_table(text)
        self.type = self.find_type("text_raw")
        self.add_effective_time("effective_time_raw")
        self.add_parameter_name_and_value("parameter_name_raw", "value_raw")
        self.add_reference_value("reference_values_raw")
        self.remove_useless_rows("value_raw")
        self.add_epi_id("epi_id")
        self.add_analysis_name("analysis_name_raw")
        self.add_parameter_unit("parameter_unit_raw")
        self.reorder_cols()

    def parse_table(self, text: str):
        text = re.sub(" +", " ", text)
        text = re.sub("\t", " ", text)
        self.parsed_table = pd.DataFrame(re.split("\n", text), columns=["text_raw"])

    def find_type(self, text_col_name):
        dates_count = sum(self.parsed_table[text_col_name].str.contains(self.date_regex, na=False))
        if dates_count == 0:
            return 7.1
        else:
            return 7.2

    def add_effective_time(self, col_name_effective_time):
        if self.type == 7.1:
            self.parsed_table[col_name_effective_time] = None
        elif self.type == 7.2:
            all_dates = self.parsed_table.text_raw.str.extract(self.date_regex)  # extact all dates from text
            date = all_dates.loc[all_dates.first_valid_index()].to_string(index=False)  # get first non null date
            self.parsed_table[col_name_effective_time] = re.search(self.date_regex, date).group()
            # remove date and time, otherwise they start messing up regexes later
            self.parsed_table["temp"] = self.parsed_table["text_raw"].str.replace(self.date_time_regex, "")

    def add_parameter_name_and_value(self, col_name_parameter_name: str, col_name_value: str):
        # splitting column 'text_raw' to text and number (correspond to 'parameter_name_raw' and 'value_raw') using regex
        if self.type == 7.1:
            self.parsed_table[[col_name_parameter_name, col_name_value]] = self.parsed_table.text_raw.str.extract(
                "([a-zA-Z-*/%#õäöü\(\)\n, ]+)([^a-zA-Z-*/%#õäöü\(\)\n, ]*)", expand=True
            )
        elif self.type == 7.2:
            self.parsed_table[[col_name_parameter_name, col_name_value]] = self.parsed_table.temp.str.extract(
                "([a-zA-Z-*/%#õäöü\(\)\n, ]+)([^a-zA-Z-*/%#õäöü\(\)\n, ]*)", expand=True
            )

    def add_reference_value(self, col_name_reference: str):
        if self.type == 7.1:
            # choose the text in the last parenthesis as reference value
            self.parsed_table = self.parsed_table.reset_index(drop=True)
            ref_df = self.parsed_table.text_raw.str.extractall(
                self.inside_paren_regex
            )  # extract text inside parentheses
            ref_df.columns = [col_name_reference]
            ref_df.index.names = ["mi1", "mi2"]  # multiindex
            self.parsed_table[col_name_reference] = ref_df.groupby("mi1", as_index=False).last().reference_values_raw
        elif self.type == 7.2:
            self.parsed_table[col_name_reference] = self.parsed_table.text_raw.str.extract("\[(.*?)\]", expand=True)

    def add_epi_id(self, col_name_epi_id: str):
        self.parsed_table[col_name_epi_id] = self.epi_id

    def remove_useless_rows(self, col_name_value: str):
        self.parsed_table = self.parsed_table[
            (self.parsed_table[col_name_value].str.strip() != "") & (~self.parsed_table[col_name_value].isnull())
        ]

    def add_analysis_name(self, col_name_analysis_name):
        self.parsed_table[col_name_analysis_name] = None

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


#
# text = '''S,P-Na 140 (136 .. 145 mmol/L )
# S,P-K 4.8 (3.4 .. 4.5 mmol/L )
# S,P-iCa 0.94 (1.17 .. 1.29 mmol/L )
# S,P-Ca 1.70 (2.15 .. 2.55 mmol/L ) '''
# epi_id = '123456'
# t7 = Type7Parser()
# result = t7(text, epi_id)
# print(result)
