import os
import pandas as pd
from newsai import _DATA_PATH, Log
from newsai.cluster import ClusterEmbed

log = Log(os.path.basename(__file__))

if __name__ == "__main__":

    file_name = 'sample_stories.csv'
    log.info(f'reading file: {file_name}')
    df = pd.read_csv(os.path.join(_DATA_PATH, file_name))

    df = df[:20]

    log.info('fitting')
    cluster = ClusterEmbed(df)
    df1 = cluster.fit()
    log.info(df1.head())

    assert len(df) == len(df1)

    log.info(df.merge(df1, left_index=True, right_index=True))
