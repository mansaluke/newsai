import sys
import os
import pandas as pd
from newsai.dfconvert import Dstore
from newsai.async_download import News
from newsai import _DATA_PATH
from newsai.utils.nlogger import Log, DEBUG

Log.set_lvl(DEBUG)
log = Log(__name__)

if __name__ == "__main__":

    m = News()
    out = m()
    df = pd.DataFrame()

    for i in out:
        df_out = pd.DataFrame(i)
        df = df.append(df_out, ignore_index=True)

    for col in ['H0', 'H1', 'H2']:
        try:
            df[col] = df[col].str.replace('\n+', '. ')
        except Exception as e:
            log.error(e)

    print(df.head())
    file_name = 'all_stories.csv'
    file_path = os.path.join(_DATA_PATH, file_name)

    try:
        Dstore(file_path).store_df(df)
    except FileExistsError:
        Dstore(file_path).append_df(df)
