import os
import pandas as pd
from newsai.async_download import HistoricalNews
from newsai import _DATA_PATH, Log
from newsai.utils.nlogger import DEBUG
from newsai.utils import nlp


Log.set_lvl(DEBUG)
log = Log(os.path.basename(__file__))

if __name__ == "__main__":

    df = pd.DataFrame()
    header_texts = ['H1', 'H2']

    for m in range(1, 7):
        log.info(f'Downloading month: {m}')
        h = HistoricalNews(year=2020, month=m)
        out = h()
        print(out.keys())
        for i in out.values():
            df_out = df.append(pd.DataFrame(i.to_pandas()))
            df = df.append(df_out, ignore_index=True)

        for col in header_texts:
            try:
                df[col] = df[col].str.replace('\n+', '. ')
            except Exception as e:
                log.error(e)

    df = nlp.remove_null_rows(df, header_texts)
    df = nlp.remove_duplicate_rows(df, ['H1', 'H2', 'alias'])
    df = nlp.remove_duplicate_columns(df, header_texts[0], header_texts[1])

    log.info('creating date column')
    df['date'] = pd.to_datetime(df['published_date']).dt.date

    log.info('selecting columns')
    df = df[header_texts + ['date', 'alias']]

    print(df.head())
    log.info('DataFrame length: {0}'.format(len(df)))

    file_name = 'sample_historicals.csv'
    file_path = os.path.join(_DATA_PATH, file_name)

    log.info(f'Writing to: ´{file_path}')
    df.to_csv(file_path, encoding='utf-8', index=False)
