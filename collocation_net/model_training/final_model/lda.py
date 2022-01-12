import sys
import pandas as pd
import numpy as np
from collections import defaultdict
from sklearn.decomposition import LatentDirichletAllocation


def lda_training(n_components, data_path="df.csv", save=True, result_path="data/"):
    data = pd.read_csv(data_path, index_col=0)

    lda = LatentDirichletAllocation(n_components=n_components, random_state=0)
    results = lda.fit_transform(data)

    if save:
        np.save(f"{result_path}/lda_row_distribution.npy", results)
        np.save(f"{result_path}/lda_column_distribution.npy", lda.components_)
        np.save(f"{result_path}/rows.npy", np.array(data.index, dtype=object))
        np.save(f"{result_path}/columns.npy", np.array(data.columns, dtype=object))

        # creating the topics where each document is assigned to the most probable topic
        topics = defaultdict(list)

        for idx, prob in enumerate(results):
            topics[np.argmax(prob)].append(data.index[idx])

        np.save(f"{result_path}/lda_topics.npy", np.array([np.array(xi) for xi in topics.values()]))

    # returns the fitted model
    return lda


if __name__ == "__main__":
    # first command line argument is the path to the collocation data file
    datafile = sys.argv[1]
    # second cmd argument is collocation type, which has to match a filename (without .csv) in the parameters folder
    collocation_type = sys.argv[2]
    # third argument is the path to the folder where the lda results should be saved
    result_path = sys.argv[3]

    params = pd.read_csv(f"../parameters/{collocation_type}.csv")
    n_components = params["value"][0]

    lda = lda_training(n_components=n_components, data_path=datafile, result_path=result_path)
