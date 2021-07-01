import psycopg2 as pg2
import os
import pandas as pd

# sys.path.append('/home/anneott/Repos/cda-data-cleaning/analysis_data_cleaning/LOINC_cleaning')


#    """Populates Pandas DataFrame with query results.
#    :param query_string: PostgreSQL query of which results are stored in the DataFrame.
#    :type query_string: str.
#    :returns:  dataframe -- dataframe filled with query results.
#    :rtype: pandas.DataFrame
#    """


def get_dataframe_from_db(query_string: str):
    connection_string = "host='p12.stacc.ee' dbname='egcut_epi' user='{postgres_username}' password='{postgres_password}'".format(
        **{"postgres_username": os.getenv("POSTGRES_USERNAME"), "postgres_password": os.getenv("POSTGRES_PASSWORD")}
    )

    with pg2.connect(connection_string) as connection:
        cursor = connection.cursor()
        cursor.execute(query_string)
        column_names = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

    return pd.DataFrame(data, columns=column_names)


def read_manually_give_data(data_list):
    columns = ["analysis_name", "parameter_code", "parameter_name", "unit", "count"]
    # data_list = [['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'MCH', 'pg', 5148],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'Hb', 'g/l', 5118],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'MCV', 'fl', 5107],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'WBC', '10\9/l', 5098],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'Plt', '10\9/l', 5093],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'MCHC', 'g/l', 5087],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'MPV', 'fl', 5086],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'P-LCR', '%', 5085],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'RBC', '10\12/l', 5008],
    #              ['Hemogramm viieosalise leukogrammiga', 'B-CBC-5Diff', 'Hct', '%', 4995]]
    df = pd.DataFrame(data=data_list, columns=columns)
    return df
