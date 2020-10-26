from abc import ABC, abstractmethod
from typing import List
import pandas as pd
from newsai.utils.nlp import list_to_str


class EmbedStrategy(ABC):
    @abstractmethod
    def fit(self, corpus: pd.DataFrame) -> pd.DataFrame:
        pass


class DefaultEmbedder(EmbedStrategy):
    def fit(self, corpus: pd.DataFrame) -> pd.DataFrame:
        from sentence_transformers import SentenceTransformer
        self.embedder = SentenceTransformer('bert-base-nli-mean-tokens')
        return pd.DataFrame(self.embedder.encode(corpus))


class Embed():

    def __init__(self,
                 df: pd.DataFrame,
                 embedstrategy: EmbedStrategy = DefaultEmbedder(),
                 header_texts: list = ['H0', 'H1', 'H2']) -> None:

        self._embedstrategy: EmbedStrategy = embedstrategy
        self.df: pd.DataFrame = df
        self.header_texts: List = header_texts

    @property
    def embedstrategy(self) -> EmbedStrategy:
        return self._embedstrategy

    @embedstrategy.setter
    def embedstrategy(self, embedstrategy: EmbedStrategy) -> None:
        self._embedstrategy = embedstrategy

    def fit(self) -> pd.DataFrame:
        self.df = self.df[self.header_texts].apply(list_to_str, axis=1)
        return self._embedstrategy.fit(self.df)
