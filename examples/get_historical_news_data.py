import sys
import os
import pandas as pd
from newsai.dfconvert import Dstore
from newsai.async_download import HistoricalNews
from newsai import _DATA_PATH, Log
from newsai.utils.nlogger import WARNING

Log.set_lvl(WARNING)
log = Log(__name__)


if __name__ == "__main__":

    df = pd.DataFrame()
    for m in range(1, 4):
        try:
            log.info(f'Downloading month: {m}')
            h = HistoricalNews(year=2020, month=m)
            out = h()
            print(out.keys())
            for i in out.values():
                df_out = df.append(pd.DataFrame(i.to_pandas()))
                df = df.append(df_out, ignore_index=True)

            for col in ['H1', 'H2']:
                try:
                    df[col] = df[col].str.replace('\n+', '. ')
                except Exception as e:
                    log.error(e)
        except Exception as e:
            log.error(e)
    print(df.head())

    file_name = 'sample_historicals.csv'
    file_path = os.path.join(_DATA_PATH, file_name)

    try:
        Dstore(file_path).store_df(df)
    except FileExistsError:
        log.error('File already exists')
        # Dstore(file_path).append_df(df)
