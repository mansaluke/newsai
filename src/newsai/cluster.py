from abc import ABC, abstractmethod
from typing import List
import pandas as pd
import numpy as np
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


class ClusterStrategy(ABC):

    @abstractmethod
    def fit(self, data: pd.DataFrame):
        pass


class DefaultCluster(ClusterStrategy):
    def fit(self, data: pd.DataFrame):
        from sklearn.cluster import AgglomerativeClustering
        clusterer = AgglomerativeClustering(
            n_clusters=None, distance_threshold=10)
        return clusterer.fit(data)


class ClusterEmbed():

    def __init__(self,
                 df: pd.DataFrame,
                 embedstrategy: EmbedStrategy = DefaultEmbedder(),
                 clusterstrategy: ClusterStrategy = DefaultCluster(),
                 header_texts: list = ['H0', 'H1', 'H2']) -> None:

        self._embedstrategy: EmbedStrategy = embedstrategy
        self._clusterstrategy: ClusterStrategy = clusterstrategy
        self.df: pd.DataFrame = df
        self.header_texts: List = header_texts

    @property
    def embedstrategy(self) -> EmbedStrategy:
        return self._embedstrategy

    @embedstrategy.setter
    def embedstrategy(self, embedstrategy: EmbedStrategy) -> None:
        self._embedstrategy = embedstrategy

    @property
    def clusterstrategy(self) -> ClusterStrategy:
        return self._clusterstrategy

    @clusterstrategy.setter
    def clusterstrategy(self, clusterstrategy: ClusterStrategy) -> None:
        self._clusterstrategy = clusterstrategy

    def fit(self) -> pd.DataFrame:
        corpus = self.df[self.header_texts].apply(list_to_str, axis=1)
        embedded_df = self._embedstrategy.fit(corpus)
        clustering = self._clusterstrategy.fit(embedded_df)
        return pd.DataFrame(
            clustering.labels_,
            # zip(corpus, clustering.labels_),
            columns=['Cluster_id'])
