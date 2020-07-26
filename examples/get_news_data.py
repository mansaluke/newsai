import sys
import os
import numpy as np
import pandas as pd
from newsai.async_download import News
from newsai import Log, _DATA_PATH
from newsai.utils import nlp

log = Log(os.path.basename(__file__))


if __name__ == "__main__":

    m = News()
    out = m()
    df = pd.DataFrame()

    for i in out.values():
        df_out = pd.DataFrame(i.to_pandas())
        df = df.append(df_out, ignore_index=True)

    duplicate_rows = df[['H0', 'H1', ]].duplicated()
    log.warning(f'Removing {len(duplicate_rows)} duplicates')
    df = df[~duplicate_rows]

    header_texts = ['H0', 'H1', 'H2']
    log.info(f'Removing sentences with a length < 3.')
    for col in header_texts:
        try:
            short_sentences = (df[col].apply(
                lambda x: len(str(x).split(' '))) <= 3)
            df.loc[short_sentences, col] = np.nan
        except Exception as e:
            log.error(e)

    df = nlp.remove_null_rows(df, header_texts)
    df = df[header_texts + ['datetime', 'url', 'alias']]

    print(df.head())
    log.info('DataFrame length: {0}'.format(len(df)))

    log.info('Shifting columns')
    df = nlp.shift_nulls(df, header_texts, _remove_null_columns=False)

    file_name = 'sample_stories.csv'
    file_path = os.path.join(_DATA_PATH, file_name)

    if not os.path.exists(file_path):
        log.info(f'Writing to: ´{file_path}')
        df.to_csv(file_path, sep=',', encoding='utf-8', index=False)
    else:
        log.info(f'Appending to: ´{file_path}')
        df.to_csv(file_path, mode='a', header=False, sep=',',
                  encoding='utf-8', index=False)
