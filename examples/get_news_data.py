import sys
import os
import pandas as pd

file_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, file_path + "/..")
from newsai.dfconvert import Dstore
from newsai.async_download import News


if __name__ == "__main__":

    m = News()  # Historicals(year=1990, month=4)
    out = m()
    df = pd.DataFrame()

    for i in out:
        df = df.append(pd.DataFrame(i))

    for col in ['H0', 'H1', 'H2']:
        try:
            df[col] = df[col].str.replace('\n+', '. ')
        except Exception as e:
            print(e)

    print(df)
    file_name = 'all_stories.csv'

    try:
        Dstore(file_name).store_df(df)
    except FileExistsError:
        Dstore(file_name).append_df(df)
