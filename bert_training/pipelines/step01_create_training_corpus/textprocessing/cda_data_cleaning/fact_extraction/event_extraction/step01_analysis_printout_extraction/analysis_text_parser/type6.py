import re
import numpy as np
import pandas as pd


# TODO if text is on one row then right now is not parsed, e.g. epi_id = '13679831'
# and text = '18.03.2009 Hemogramm WBC 8.28 (3,5 .. 8,8 E9/L ) RBC 3.48 (3,9 .. 5,2 E12/L ) ...'

# type 6.1 example epi_id-s :  18619340, 18798396, 35458279
# type 6.2 example epi_id-s :  99344174, 99619203, 56618642


class Type6Parser:
    def __init__(self):
        self.analysis_name_regex = (
            r"[Hh]emogramm|[Uu]riini|[Vv]ereäige|[Bb]iokeemia|[Kk]liinilise keemia|[Hh]appe-aluse tasakaal"
        )
        # type 6 contains a lot of noise, so the day in date regex must be stricter
        self.date_regex = r"(0[1-9]|[12][0-9]|3[01]|[0-9])\.\d{2}\.\d{4}"
        self.date_time_regex = r"\d{1,2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}:"
        self.an_rows_to_remove = None

    def __call__(self, text: str, epi_id: str):
        self.epi_id = epi_id
        self.parse_type6(text)
        return self.parsed_table

    def parse_type6(self, text: str):
        self.parse_table(text)
        self.add_analysis_name("analysis_name_raw", "text_raw")
        self.type = self.find_type("text_raw")
        self.add_effective_time("effective_time_raw", "text_raw")
        self.add_reference_value("reference_values_raw")
        self.add_parameter_name_and_value("parameter_name_raw", "value_raw", "text_raw")
        self.remove_useless_rows("value_raw")
        self.add_epi_id("epi_id")
        self.add_parameter_unit("parameter_unit_raw")
        self.reorder_cols()

    def parse_table(self, text):
        text = re.sub(" +", " ", text)
        self.parsed_table = pd.DataFrame(re.split("\n", text), columns=["text_raw"])

    def find_type(self, text_col_name):
        dates_count = sum(self.parsed_table[text_col_name].str.contains(self.date_time_regex, na=False))
        # date in the beginning
        # e.g.
        # 04.03.2010
        # hemogramm viieosalise leukogrammiga
        # WBC 6.99 (3,5 .. 8,8 E9/L )
        if dates_count <= 2:
            return 6.1
        # date on every row e.g. 11.03.2019 11:11: Kreatiniin 96 µmol/L [norm 59 - 104]
        else:
            return 6.2

    def find_idx(self, df, pattern, column):
        # finds row indices that match with given pattern in column parameter name raw
        mask = df[[column]].apply(lambda x: x.str.contains(pattern, regex=True)).any(axis=1)
        return df[mask].index

    def add_analysis_name(self, col_name_analysis, col_name_text):
        self.parsed_table = self.parsed_table[self.parsed_table[col_name_text] != ""].reset_index(drop=True)

        # find indices of the rows containing analysis names
        # text_col = self.parsed_table[col_name_text]
        # an_idxs = text_col.loc[text_col.str.extract(self.analysis_name_regex, expand=False).notnull()].index
        an_idxs = self.find_idx(self.parsed_table, self.analysis_name_regex, col_name_text)
        self.an_rows_to_remove = an_idxs

        if an_idxs.size:
            # analysis names span over multiple columns
            an_names_str = self.parsed_table.iloc[an_idxs, :].to_string(header=False, index=False)
            # make list of all the analysis names the table contains
            an_names_list = [an_names_str] if len(an_idxs) == 1 else an_names_str.split("\n")
            an_idxs = np.append(an_idxs, len(self.parsed_table) - 1)  # adding as the last index the last row of table

            # assigne corresponding analysis names to each row
            # sometimes it contains date, regex '' excludes that
            for i in range(len(an_names_list)):
                self.parsed_table.loc[an_idxs[i] : an_idxs[i + 1], col_name_analysis] = re.sub(
                    self.date_regex, "", an_names_list[i]
                )

        # no analysis_names
        else:
            self.parsed_table[col_name_analysis] = None  # add empty column

    def add_effective_time(self, col_name_effective_time, col_name_text):
        # date not on every line
        if self.type == 6.1:
            # find indices of the rows containing effective times
            # text_col = self.parsed_table[col_name_text]
            # et_idxs_ini = text_col.loc[text_col.str.extract('(' + self.date_regex + ')', expand=False).notnull()].index
            et_idxs_ini = self.find_idx(self.parsed_table, self.date_regex, col_name_text)

            if et_idxs_ini.size:
                et_list = list(self.parsed_table.iloc[et_idxs_ini, 0])  # .to_string(header=False, index=False)
                et_idxs = np.append(et_idxs_ini, len(self.parsed_table) - 1)

                # assigne corresponding effective times to each row, extract only date from the row
                for i in range(len(et_list)):
                    self.parsed_table.loc[et_idxs[i] : et_idxs[i + 1], col_name_effective_time] = re.search(
                        self.date_regex, et_list[i]
                    ).group()

            # no effective time
            else:
                self.parsed_table[col_name_effective_time] = None

            # remove rows that contain only analysis name or effective time
            # otherwise they multiply during conversion from wide to long in next step
            rows_to_remove = list(set(self.an_rows_to_remove).union(set(et_idxs_ini)))
            self.parsed_table = self.parsed_table.drop(rows_to_remove, axis=0).reset_index(drop=True)

        # each row has date in the beginning
        elif self.type == 6.2:
            all_dates = self.parsed_table.text_raw.str.extract(
                "(" + self.date_regex + ")"
            )  # extact all dates from text
            date = all_dates.loc[all_dates.first_valid_index()].to_string(index=False)  # get first non null date
            self.parsed_table[col_name_effective_time] = re.search(self.date_regex, date).group()
            # remove date and time, otherwise they start messing up regexes later
            self.parsed_table["temp"] = self.parsed_table["text_raw"].str.replace(self.date_time_regex, "")

    def add_parameter_name_and_value(self, col_name_parameter_name, col_name_value, col_name_text):
        if self.type == 6.1:
            # for integer/float values
            # splitting column 'text_raw' to  text and number (correspond to 'parameter_name_raw' and 'value_raw') using regex
            self.parsed_table[[col_name_parameter_name, col_name_value]] = self.parsed_table.text_raw.str.extract(
                "([a-zA-Z-*/%#õäöüÕÄÖÜ\(\)\n, ]+)([^a-zA-Z-*/%#õäöüÕÄÖÜ\(\)\n, ]*)", expand=True
            )
            # for text values
            # extract the information before the parenthesis e.g. 'Glükoos POS (NEG )' -> 'POS'
            text_values = self.parsed_table[col_name_text].str.extract("^[\dA-Za-zõäöüÕÄÖÜ-]+\s*([^\(]+)", expand=True)
            text_values.columns = [col_name_value]
            # assign to rows where there are no numbers in text e.g. Glükoos NEG (NEG )
            self.parsed_table.loc[
                self.parsed_table[col_name_text].str.contains("^[^0-9]+$"), [col_name_value]
            ] = text_values  # df['text_raw'].str.extract("^[\dA-Za-zõäöüÕÄÖÜ-]+\s*([^\(]+)", expand=True)#df['A'].str.rsplit(r' ', 1, expand=True)
            self.parsed_table.loc[
                self.parsed_table[col_name_text].str.contains("^[^0-9]+$"), [col_name_parameter_name]
            ] = self.parsed_table.text_raw.str.split(" ", 1, expand=True)
        elif self.type == 6.2:
            # remove "a + <number combination>", otherwises messes up regex logic
            self.parsed_table["temp"] = self.parsed_table["temp"].str.replace("a\d{4}", "")
            # extract parameter name and value
            # parameter name is everything before space and number e.g. "WBC 3.5 E9/L" => "WBC" "ft4 3.5 E9/L" => "ft4"
            self.parsed_table[col_name_parameter_name] = self.parsed_table["temp"].str.extract("(.*\s)\d", expand=True)
            self.parsed_table[col_name_value] = self.parsed_table["temp"].str.extract("\s(\d+\.?,?\d*)", expand=True)

    def add_parameter_unit(self, col_name_parameter_unit):
        if self.type == 6.1:
            self.parsed_table[col_name_parameter_unit] = None
        if self.type == 6.2:
            # unit is everything that comes from number and space e.g. "WBC 3.5 E9/L" => "E9/L" "ft4 3.5 mmol" => "mmol"
            self.parsed_table[col_name_parameter_unit] = self.parsed_table["temp"].str.replace("(.*)\d\s", "")

    def add_reference_value(self, col_name_reference):
        # for empty table do not add anything
        if not self.parsed_table.text_raw.empty:
            if self.type == 6.1:
                self.parsed_table[col_name_reference] = self.parsed_table.text_raw.str.extract(
                    "\((.*?)\)", expand=True
                )
            elif self.type == 6.2:
                self.parsed_table[col_name_reference] = self.parsed_table.text_raw.str.extract(
                    "\[(.*?)\]", expand=True
                )
                # remove reference values, otherwise they start messing up regexes later
                self.parsed_table["temp"] = self.parsed_table["temp"].str.replace("\[(.*?)\]", "")  # remove ref value

        # TODO indices get messed up when contain rows that do not have ( )
        # choose the text in the last parenthesis as reference value
        # self.parsed_table =  self.parsed_table.reset_index(drop=True)
        # ref_df = self.parsed_table.text_raw.str.extractall('\((.*?)\)') # extract text inside parentheses
        # ref_df.columns = ['reference_values_raw']
        # ref_df.index.names = ['mi1','mi2']  # multiindex
        # self.parsed_table['reference_values_raw'] = ref_df.groupby('mi1', as_index=False).last().reference_values_raw

    def add_epi_id(self, col_name_epi_id):
        self.parsed_table[col_name_epi_id] = self.epi_id

    def remove_useless_rows(self, col_name_value):
        if not self.parsed_table[col_name_value].empty:
            self.parsed_table = self.parsed_table[
                (self.parsed_table[col_name_value].str.strip() != "") & (~self.parsed_table["value_raw"].isnull())
            ]

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
# epi_id = "123456"
# text = """hemogramm viieosalise leukogrammiga
# WBC 4.67 (3,5 .. 8,8 E9/L )
# RBC 4.32 (4,2 .. 5,7 E12/L )
#
# 30.06.2010
# S,P-Uurea 3.3 (&lt;8.3 mmol/L )
# S,P-CRP 2 (&lt;5 mg/L )    """
# t6 = Type6Parser()
# result = t6(text, epi_id)
# print(result)
