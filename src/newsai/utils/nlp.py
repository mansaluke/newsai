from typing import Union, Optional, AnyStr, List
import math
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


def split_on_uppercase(string_input: AnyStr) -> List:
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


def activate_nltk() -> set:
    global stop_words
    nltk.download('wordnet')
    nltk.download('stopwords')
    nltk.download('punkt')
    stop_words = set(stopwords.words('english'))
    return stop_words


def series_to_string(text_series: pd.Series):
    return text_series.to_string(index=False).replace("\n", "")


def get_non_stop_words(text_data: Union[pd.Series, str]):
    if type(text_data) == pd.Series:
        text_data = series_to_string(text_data)
    non_stop_words = [
        lemmatizer.lemmatize(w)
        for w in re.findall(r'\b\S+\b', text_data.casefold())
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
        return \
            [lemmatizer.lemmatize(w)
             for w in re.findall(r'\b\S+\b', text.lower())
             if w not in stop_words]

    return split_words(text)


def is_null(val) -> bool:
    # try:
    #     return math.isnan(val)
    # except TypeError:
    #     return False
    return val != val


def remove_null_rows(df: pd.DataFrame, columns: list):
    """
    removes rows with all nulls in the columns specified
    """
    null_rows = (df[columns].isna()).all(axis=1)

    log.warning(f'Removing {df[null_rows].size} rows with nulls')
    return df[~null_rows]


def remove_null_columns(df: pd.DataFrame, headers_to_check: list
                        ) -> pd.DataFrame:
    for h in headers_to_check:
        if len(df[df[h].notna()]) == 0:
            log.info(f'Removing column {h}')
            df = df.drop(columns=h)
    return df


def shift_nulls(df: pd.DataFrame, headers: list,
                _remove_null_columns=True) -> pd.DataFrame:
    """
    shifts column values to the left to occupy nulls e.g.
        H0  H1  H2
    1   nan nan a
    2   nan b   c
    becomes
        H0  H1  H2
    1   a   nan nan
    2   b   c   nan
    and if _remove_null_columns is true we are left with:
        H0  H1
    1   a   nan
    2   b   c 
    """
    df = df.T
    for c in df.columns:
        for k, v in enumerate(headers):
            if not is_null(df[c][v]):
                if k > 0:
                    for i in range(len(headers)):
                        if k+i < len(headers):
                            df[c][i] = df[c][k+i]
                break
    if not _remove_null_columns:
        return df.T
    else:
        print(headers)
        return remove_null_columns(df.T, headers)
