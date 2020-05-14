import sys
import os
import pandas as pd
from newsai.dfconvert import Dstore
from newsai.async_download import HistoricalNews
from newsai import _DATA_PATH
from newsai.utils.nlogger import Log

log = Log(__name__)


if __name__ == "__main__":

    df = pd.DataFrame()
    for m in range(1, 4):
        try:
            log.info(f'Downloading month: {m}')
            m = HistoricalNews(year=2020, month=m)
            out = m()

            for i in out:
                df = df.append(pd.DataFrame(i))

            for col in ['H1', 'H2']:
                try:
                    df[col] = df[col].str.replace('\n+', '. ')
                except Exception as e:
                    log.error(e)
        except Exception as e:
            log.error(e)
    print(df.head())

    file_name = 'hist.csv'
    file_path = os.path.join(_DATA_PATH, file_name)

    try:
        Dstore(file_path).store_df(df)
    except FileExistsError:
        Dstore(file_path).append_df(df)
