import os
import pathlib
from common.read_and_update import DatabaseConnector
from cda_data_cleaning.data_cleaning.diagnosis_data_cleaning.step01_clean_diagnosis_table.cleaning import Cleaner, Report
from sqlalchemy import and_

cleaner = Cleaner()

directory = pathlib.Path(__file__).parent.as_posix()
report = Report(os.path.join(directory, "../reports/"))


def clean(row):
    result_dict = {}
    cleaned = cleaner.clean(row["diag_code_raw"], row["diag_name_raw"], row["diag_statistical_type_raw"])
    result_dict["diag_code"] = cleaned.code
    result_dict["diag_name"] = cleaned.name
    result_dict["diag_name_additional"] = cleaned.additional
    result_dict["cleaning_state"] = cleaned.cleaning_status
    result_dict["diag_statistical_type"] = cleaned.statistical_type
    result_dict["clean"] = cleaned.clean

    report.log(cleaned)

    return result_dict


def db_filter(dc, start_over=True):
    """ Creates a filter for table rows

    Args:
        dc (DatabaseConnector): databaseconnector that we need to access the table
        start_over (bool): do we start from beginning or take the start id in the log file

    Returns:
        filter for table rows
    """
    if start_over:
        dc.start = 0
    my_filter = True
    start_filter = dc.table.c["id"] >= dc.start
    return and_(my_filter, start_filter)


def main(prefix, work_schema, target_table, database_connection_string):
    table_name = work_schema + "." + target_table

    original = ["diag_code_raw", "diag_name_raw", "diag_text_raw", "diag_statistical_type_raw"]
    new = ["diag_code", "diag_name", "diag_name_additional", "diag_statistical_type", "cleaning_state", "clean"]
    start_over = True
    id_field_name = "id"

    log_file_name = os.path.join(directory, "../reports/log_id.txt")

    dc = DatabaseConnector(database_connection_string, table_name, log_name=log_file_name)
    my_filter = db_filter(dc, start_over=start_over)
    result = dc.process_rows(clean, id_column_name=id_field_name, original=original, new=new, my_filter=my_filter)
    if result:
        print("Updating was successful!")
        print("-------------------------------------")
        report.report(prefix)
    else:
        print("Unexpected error occurred! Updated only rows up to id: " + str(dc.get_start_id()) + ".")
