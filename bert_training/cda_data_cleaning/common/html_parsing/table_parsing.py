import lxml
import numpy as np
from lxml import html
import re
import pandas as pd
from typing import List


def parse_body(tbody: lxml.html.HtmlElement, cols=None) -> pd.DataFrame:
    """
    Parses html table body to pandas DataFrame

    :param tbody: lxml node corresponding to HTMl table body
    :param cols: expected number of columns in the HTML table
    :returns filled dataframe according to HTML table

    Parsing takes into account rowspan and colspan attributes.
    If cells spans over several rows and columns then the value is copied to
    corresponding cells in the pandas dataframe.
    All empty strings are changed to None.

    Bugs:
    Cannot correctly handle if all cells in the row have rowspan larger than one.
    And other similar errors.
    These are invalid HTML tables anyway.
    """
    assert tbody.tag == "tbody"

    # kui cols = None siis võtab columnite väärtuse body põhjal ja võib tagastab väiksema tabeli kui meie algne df
    if cols is None:
        cols = count_body_columns(tbody)

    if count_body_rows(tbody) == 0 or count_body_rows(tbody) is None:
        status = "no body rows"

    row_count = count_body_rows(tbody)
    rows = range(0, row_count)

    # df = pd.DataFrame(None, index=rows, columns=range(0, cols)).astype(str)
    # df.iloc[:, :] = None
    df = np.full([row_count, cols], None, dtype=object)

    for i1, row in enumerate(tbody.xpath("tr")):
        j1 = 0
        for el in row.xpath("td"):

            # finds first empty cell
            while j1 < cols and df[i1, j1] is not None:
                j1 += 1

            # check that the cell is in within the table
            if j1 > cols:
                break

            # fills the empty cell
            i2 = min(i1 + int(el.get("rowspan", 1)), rows[-1] + 1)  # m[-1] + 1 number of rows
            j2 = min(j1 + int(el.get("colspan", 1)), cols)

            if el.text == "":
                el.text = None

            df[i1:i2, j1:j2] = el.text

            if i2 > rows[-1] + 1:
                status = "out of row bounds"
                print(status)
            elif j2 > cols:
                status = "out of column bounds"
                print(status)

            j1 += 1

    return pd.DataFrame(df)


def is_date(html_date: str):
    if re.match("^\d{2}\.\d{2}\.\d{4}$", html_date):
        return True
    elif re.match("^\d{4}\.\d{2}\.\d{2}$", html_date):
        return True
    elif re.match("^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$", html_date):
        return True
    else:
        return False


def is_simple_column(column: lxml.html.HtmlElement):
    # checks if colspan is 1
    assert column.tag == "th"

    colspan = int(column.get("colspan", 1))
    if colspan == 1:
        return True
    else:
        return False


def are_simple_columns(header_cells: lxml.html.HtmlElement):
    assert header_cells.tag == "th"

    for i in range(0, len(header_cells)):
        if not is_simple_column(header_cells[i]):
            return False
    return True


def count_body_rows(tbody: lxml.html.HtmlElement):
    if tbody.tag != "tbody":
        return None
    count = 0
    for table_row in tbody.xpath("tr"):
        # no cell values (td) in table row
        if not table_row.xpath("td"):
            continue
        count += 1
    return count


def count_body_columns(tbody: lxml.html.HtmlElement):
    if tbody.tag != "tbody":
        return None
    count = 0
    first_body_row = tbody.xpath("tr")[0]
    for el in first_body_row.xpath("td"):
        colspan = int(el.get("colspan", 1))
        count += colspan
    return count


# def count_body_columns(tbody: lxml.html.HtmlElement):
#     if tbody.tag != 'tbody':
#         return None
#     first_body_row = tbody.xpath('tr')[0]
#     column_number = len(first_body_row.xpath('td'))
#     return column_number


def count_header_rows(table: lxml.html.HtmlElement):
    if table.tag != "table":
        return None
    header_cells = table.xpath("thead")[0].xpath("tr/th")
    header_rows = int(header_cells[0].get("rowspan", 1))
    return header_rows


def get_labels(table: lxml.html.HtmlElement):  # -> List[str]:
    table.tag == "table"

    if len(table.xpath("thead")) != 1:
        return None

    # Returns list of labels extracted from the table heading

    # Tables contain two types of headings:
    # (1) simple one row headings (8172732318)
    # (2) headings with two rows (154644958, Example 2.1)

    # All values are simply returned as a list in case of one row heading.
    # Two row headings are flattened. For example:

    # | Analüüs | Parameeter | Ühik | Referents |      Tulemused    |
    # | ------- | ---------- | ---- | --------- | Kuupäev | Tulemus |

    # becomes: [Analüüs, Parameeter, Ühik, Referents, Kuupäev, Tulemus]

    labels = []
    thead = table.xpath("thead")[0]

    for header_cell in thead.xpath("tr/th"):

        colspan = int(header_cell.get("colspan", 1))
        if colspan > 1:  # if header has two rows, then we only want the values from the lower row
            continue

        # lisan ka tühjad labelid
        if header_cell.text is not None:
            labels.append(header_cell.text.capitalize())
        else:
            labels.append("")

    return labels
