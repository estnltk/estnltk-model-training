import sqlite3
import pandas as pd
import networkx as nx
from typing import List


def fetch_entries(db_name: str, table_name: str) -> List[tuple]:
    """
    Fetches and returns all entries from a given table in a given database.

    :param db_name: the name of the sqlite database file
    :param table_name: the name of the table in the database that contains
                        the collocations
    :return: all entries in the form of (lemma1, pos1, lemma2, pos2, total)
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute(f"SELECT lemma1, pos1, lemma2, pos2, total FROM {table_name} WHERE total > 2;")
    entries = cur.fetchall()
    return entries


def connected_entries(all_entries: List[tuple], first_words: List[str], second_words: List[str]) -> List[tuple]:
    """
    Finds all collocations where words are connected to at least some other words.
    Collocations that have no other connections or are in very small separate
    connected components are removed as they do not cluster well due to said lack of
    connections.

    :param all_entries: list of all collocation pairs in the form of [lemma1, pos1, lemma2, pos2, count]
    :param first_words: list of non-duplicate words that come first in the collocations (ie nouns in noun-adj pairs)
    :param second_words: list of non-duplicate words that come second in the collocations (ie adjectives in noun-adj pairs)
    :return: list of entries that belong to the largest connected component
    """
    graph = nx.Graph()
    graph.add_nodes_from(first_words)
    graph.add_nodes_from(second_words)
    graph.add_edges_from([(entry[0], entry[2]) for entry in all_entries])
    largest_subgraph = graph.subgraph(max(nx.connected_components(graph), key=len)).copy()
    connected_words = largest_subgraph.nodes()
    connected = [entry for entry in all_entries if entry[0] in connected_words and entry[2] in connected_words]
    return connected


def matrix_creation(entries: List[tuple], first_words: List[str], second_words: List[str], save_to_csv: bool = False, filename: str = "collocation_df.csv") -> pd.DataFrame:
    """
    Creates a Pandas DataFrame from a list of collocations.

    :param entries: list of final collocations, each list element must be a list/tuple in the following format:
                    (lemma1, pos1, lemma2, pos2, count)
    :param first_words: list of non-duplicate words used for the index
    :param second_words: list of non-duplicate words used for the column names
    :param save_to_csv: boolean value that determines whether the created dataframe will be saved to a csv file,
                        default value is False
    :param filename: specifies the filename if save_to_csv is True
    :return:
    """
    data = pd.DataFrame(0, index=first_words, columns=second_words)
    data_dict = data.to_dict()

    for tup in entries:
        # += count because some pairs can appear several times with different postags
        data_dict[tup[2]][tup[0]] += tup[4]

    data_final = pd.DataFrame(data_dict)

    if save_to_csv:
        data_final.to_csv(filename)

    return data_final
