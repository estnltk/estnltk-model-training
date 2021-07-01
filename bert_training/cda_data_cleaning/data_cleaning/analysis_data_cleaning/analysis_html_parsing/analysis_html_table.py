import os
import lxml

from lxml import html
from IPython.core.display import display, HTML
from typing import NamedTuple
from typing import Callable

from cda_data_cleaning.common.html_parsing import parse_body, get_labels, count_header_rows, count_body_columns

from .type_1 import get_subtype_for_type1, parse_type1
from .type_2 import get_subtype_for_type2, parse_type2
from .type_3 import get_subtype_for_type3, parse_type3
from .type_4 import get_subtype_for_type4, parse_type4
from .type_5 import get_subtype_for_type5, parse_type5


class ConfRow(NamedTuple):
    type: str
    row_count: int
    header_test: Callable
    table_transformation: Callable


class AnalysisHtmlTable:
    def __init__(self, html_table: lxml.html.HtmlElement, epi_id: str, epi_type: str, panel_id: int):
        """
        Parses html table into pandas DataFrame
        Determines table type (1, 2.1, 2.2, 2.3, 2.4, 3, 4)
        Transforms all table types to same form with columns
           ['row_nr', 'epi_id','panel_id','parse_type','epi_type',	'analysis_name_raw','parameter_name_raw',
           'parameter_unit_raw','reference_values_raw','effective_time_raw','value_raw','analysis_substrate_raw']
        All empty values are changed to None

        html_table (lxml.html.HtmlElement): one html table
        epi_id (str): corresponding epi_id to html table
        epi_type (str): corresponding type, either 's' or 'a'
        panel_id (itn): given panel id to html table
        """

        self.parsing_conf = [
            ConfRow("Type 1", 1, get_subtype_for_type1, parse_type1),
            ConfRow("Type 2", 2, get_subtype_for_type2, parse_type2),
            ConfRow("Type 3", 1, get_subtype_for_type3, parse_type3),
            ConfRow("Type 4", 1, get_subtype_for_type4, parse_type4),
            ConfRow("Type 5", 1, get_subtype_for_type5, parse_type5),
        ]

        self.status = False
        self.df = None
        self.parse_type = None
        self.error = []
        self.meta = []  # meta exists only for type 4

        try:
            self.table = html_table
            self.tbody = self.table.xpath("tbody")[0]
            self.labels = get_labels(self.table)

            # invalid HTML table
            # number of body columns does not match with number of header columns
            if len(self.labels) != count_body_columns(self.tbody):
                self.meta.append("Invalid HTML, number of body columns not equal to number of header columns")
                self.error.append("Invalid HTML, number of body columns not equal to number of header columns")
                return

            # Extract dataframe and assign column labels
            self.df = parse_body(self.tbody)

            self.header_rows = count_header_rows(self.table)
        except:
            self.parse_type = None
            self.error.append("Invalid HTML table")
            return

        # determine parse type and transform data to canonical form
        for index, row in enumerate(self.parsing_conf):
            if row.row_count != self.header_rows:
                continue
            try:
                self.parse_type = row.header_test(self.table, self.labels)
            except:
                self.error.append("Row header test failed for parsing_conf row {}".format(index))
                continue

            if self.parse_type:
                self.df = row.table_transformation(self.df, self.labels, self.parse_type, self.meta)
                self.df.insert(
                    loc=0, column="row_nr", value=range(1, self.df.shape[0] + 1)
                )  # row numbers as first column
                self.df.insert(loc=1, column="epi_id", value=epi_id)  # epi_id as second column
                self.df.insert(loc=2, column="panel_id", value=panel_id)  # panel_id as third column
                self.df.insert(loc=3, column="epi_type", value=epi_type)
                self.df.insert(loc=3, column="parse_type", value=self.parse_type)
                self.status = True

                # information to log, if long type of table (type 2) has value = '-' it will be deleted afterwards
                # example epi_id = 48768550
                if str(self.parse_type).startswith("2") and any(self.df["value_raw"] == "-"):
                    self.error.append('Warning: Long table has value = "-" which will be deleted during cleaning')

                return

        self.error.append("Unknown table type")

    @staticmethod
    def show_example(subtype: int, table_format="html"):
        fname = "{}/example_type_{}.html".format(os.path.dirname(os.path.abspath(__file__)), subtype)
        with open(fname) as file:
            content = file.read()
        if table_format == "html":
            return display(HTML(content))
        else:
            return print(content)

    def _repr_html_(self):
        if self.parse_type is None:
            print("Parsing failed: ", self.error)
        return self.df
