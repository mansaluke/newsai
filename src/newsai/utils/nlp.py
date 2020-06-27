from typing import Union, Optional
from collections import Counter
import pandas as pd
import re
import nltk
from nltk import ngrams
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from re import sub
from .nlogger import Log

log = Log(__name__)


stop_words = set()
lemmatizer = WordNetLemmatizer()


def split_on_uppercase(string_input):
    matches = [
        match.span()[0]+1 for match in re.finditer(
            re.compile(r'([\[a-z0-9][A-Z]|[\[a-zA-Z0-9][A-Z][a-z0-9])'),
            string_input)]
    matches.insert(0, 0)
    matches.append(len(string_input))
    out = []
    for i in range(len(matches)-1):
        out.append(string_input[matches[i]: matches[i+1]])
    return out


def activate_nltk():
    global stop_words
    nltk.download('wordnet')
    nltk.download('stopwords')
    nltk.download('punkt')
    stop_words = set(stopwords.words('english'))
    return stop_words


# try:
#     activate_nltk()
# except Exception as e:
#     log.warning(
#         f'Could not activate nltk. no stop words defined. Exception: {e}')


symbol_map = {r"[^A-Za-z0-9^,!?.\/'+]": " ",
              r"\+": " plus ",
              r",": " ",
              r"\.": " ",
              r"!": " ! ",
              r"\?": " ? ",
              # r"'": " ",
              r"\"": " ",
              r":": " : ",
              r"\s{2,}": " "}


def remove_null_rows(df: pd.DataFrame, columns: list):
    """
    removes rows with all nulls in the columns specified
    """
    null_rows = (df[columns].isna()).all(axis=1)

    log.info(f'Removing {df[null_rows].size} rows with nulls')
    return df[~null_rows]


def series_to_string(text_series: pd.Series):
    return text_series.to_string(index=False).replace("\n", "")


def get_non_stop_words(text_data: Union[pd.Series, str]):
    if type(text_data) == pd.Series:
        text_data = series_to_string(text_data)
    non_stop_words = [
        lemmatizer.lemmatize(w) for w in re.findall(r'\b\S+\b', text_data.casefold())
        if w not in stop_words]
    return non_stop_words


def get_total_word_count(text_data: Union[pd.Series, str]):
    return len(get_non_stop_words(text_data))


def get_top_n_words(text_data: Union[pd.Series, str],
                    n_top_words: Optional[int] = None, ngram: int = 1):
    """
    we can use xgram variable to specify reocurring words
    """
    non_stop_words = get_non_stop_words(text_data)
    xgram = ngrams(non_stop_words, ngram)
    return Counter(xgram).most_common(10)


def text_to_word_list(text, symb_map: dict = symbol_map):
    text = str(text).lower()

    for k, v in symb_map.items():
        text = sub(k, v, text)

    def split_words(text_data):
        return [lemmatizer.lemmatize(w) for w in re.findall(r'\b\S+\b', text.lower()) if w not in stop_words]

    return split_words(text)


# def download_url(url:str, dest:str, overwrite:bool=False, pbar:ProgressBar=None,
#                  show_progress=True, chunk_size=1024*1024, timeout=4, retries=5)# ->None:
#     "Download `url` to `dest` unless it exists and not `overwrite`."
#     if os.path.exists(dest) and not overwrite: return
#
#     s = requests.Session()
#     s.mount('http://',requests.adapters.HTTPAdapter(max_retries=retries))
#     # additional line to identify as a firefox browser, see #2438
#     s.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:71.0) # Gecko/20100101 Firefox/71.0'})
#     u = s.get(url, stream=True, timeout=timeout)
#     try: file_size = int(u.headers["Content-Length"])
#     except: show_progress = False
#
#     with open(dest, 'wb') as f:
#         nbytes = 0
#         if show_progress: pbar = progress_bar(range(file_size), leave=False, # parent=pbar)
#         try:
#             if show_progress: pbar.update(0)
#             for chunk in u.iter_content(chunk_size=chunk_size):
#                 nbytes += len(chunk)
#                 if show_progress: pbar.update(nbytes)
#                 f.write(chunk)
#         except requests.exceptions.ConnectionError as e:
#             fname = url.split('/')[-1]
#             from fastai.datasets import Config
#             data_dir = Config().data_path()
#             timeout_txt =(f'\n Download of {url} has failed after {retries} retries\n'
#                           f' Fix the download manually:\n'
#                           f'$ mkdir -p {data_dir}\n'
#                           f'$ cd {data_dir}\n'
#                           f'$ wget -c {url}\n'
#                           f'$ tar -zxvf {fname}\n\n'
#                           f'And re-run your code once the download is successful\n')
#             print(timeout_txt)
#             import sys;sys.exit(1)
