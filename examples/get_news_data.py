import sys
import os
import numpy as np
import pandas as pd
from newsai.dfconvert import Dstore
from newsai.async_download import News
from newsai import Log, _DATA_PATH
from newsai.utils import nlp

log = Log(__name__)


if __name__ == "__main__":

    m = News()
    out = m()
    df = pd.DataFrame()

    for i in out.values():
        df_out = pd.DataFrame(i.to_pandas())
        df = df.append(df_out, ignore_index=True)
        df.drop_duplicates(subset=None, keep='first', inplace=True)

    header_texts = ['H0', 'H1', 'H2']
    log.info(f'Removing sentences with a length < 3.')
    for col in header_texts:
        try:
            short_sentences = (df[col].apply(lambda x: len(str(x).split(' '))) <= 3)
            df.loc[short_sentences, col]=np.nan
        except Exception as e:
            log.error(e)

    df = nlp.remove_null_rows(df, header_texts)

    print(df.head())
    log.info('DataFrame length: {0}'.format(len(df)))
    file_name = 'sample_stories.csv'
    file_path = os.path.join(_DATA_PATH, file_name)

    try:
        Dstore(file_path).store_df(df)
    except FileExistsError:
        Dstore(file_path).append_df(df)
