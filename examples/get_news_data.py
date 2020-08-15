import os
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
        df_out = i.to_pandas()
        df = df.append(df_out, ignore_index=True)

    header_texts = ['H0', 'H1']

    df = nlp.remove_short_sentences(df, header_texts, 3)
    df = nlp.remove_null_rows(df, header_texts)

    log.info('Shifting columns')
    df = nlp.shift_nulls(df, header_texts, _remove_null_columns=False)
    # lets concatenate any extra info from column H2 into H1.
    df['H1'] = df[['H1',  'H2']].apply(lambda x: ' '.join(x.dropna()), axis=1)
    df['H1'] = nlp.truncate_long_sentences(df['H1'], 30)

    df = nlp.remove_duplicate_rows(df, ['H0', 'H1', 'alias'])
    df = nlp.remove_duplicate_columns(df, header_texts[0], header_texts[1])

    log.info('creating date column')
    df['date'] = df['datetime'].dt.date

    log.info('selecting columns')
    df = df[header_texts + ['date', 'alias']]

    print(df.head())
    log.info('DataFrame length: {0}'.format(len(df)))

    file_name = 'sample_stories.csv'
    file_path = os.path.join(_DATA_PATH, file_name)

    if not os.path.exists(file_path):
        log.info(f'Writing to: ´{file_path}')
        df.to_csv(file_path, sep=',', encoding='utf-8', index=False)
    else:
        log.info(f'Appending to: ´{file_path}')
        df.to_csv(file_path, mode='a', header=False, sep=',',
                  encoding='utf-8', index=False)
