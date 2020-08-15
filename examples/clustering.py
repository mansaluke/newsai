import os
import pandas as pd
from newsai import _DATA_PATH, Log
from newsai.embed import Embed
from newsai.cluster import Cluster

log = Log(os.path.basename(__file__))

if __name__ == "__main__":

    file_name = 'sample_stories.csv'
    log.info(f'reading file: {file_name}')
    df = pd.read_csv(os.path.join(_DATA_PATH, file_name))

    df = df[:10]

    log.info('embedding')
    corpus = Embed(df, header_texts=['H0', 'H1']).fit()
    print(corpus)

    log.info('Clustering')
    df1 = Cluster(corpus).fit()
    log.info(df1.head())

    log.info(df.merge(df1, left_index=True, right_index=True))
