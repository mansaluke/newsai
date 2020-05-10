import sys
import os
import pandas as pd

file_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, file_path + "/..")
from newsai.dfconvert import Dstore
from newsai.async_download import HistoricalNews


if __name__ == "__main__":

    df = pd.DataFrame()
    for m in range(1, 4):
        try:
            print(m)
            m = HistoricalNews(year=2020, month=m)
            out = m()

            for i in out:
                df = df.append(pd.DataFrame(i))
                print(df)
            for col in ['H1', 'H2']:
                try:
                    df[col] = df[col].str.replace('\n+', '. ')
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
    print(df.head())

    file_name = 'hist.csv'

    try:
        Dstore(file_name).store_df(df)
    except FileExistsError:
        Dstore(file_name).append_df(df)
