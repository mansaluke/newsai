from abc import ABC, abstractmethod
from typing import List
import pandas as pd
import numpy as np
from newsai.utils.nlp import list_to_str


class ClusterStrategy(ABC):

    @abstractmethod
    def fit(self, data: pd.DataFrame):
        pass


class DefaultCluster(ClusterStrategy):
    def fit(self, data: pd.DataFrame):
        from sklearn.cluster import AgglomerativeClustering
        clusterer = AgglomerativeClustering(
            n_clusters=None, distance_threshold=5)
        return clusterer.fit(data)


class Cluster():

    def __init__(self,
                 corpus: pd.DataFrame,
                 clusterstrategy: ClusterStrategy = DefaultCluster()
                 ) -> None:

        self._clusterstrategy: ClusterStrategy = clusterstrategy
        self.corpus: pd.DataFrame = corpus

    @property
    def clusterstrategy(self) -> ClusterStrategy:
        return self._clusterstrategy

    @clusterstrategy.setter
    def clusterstrategy(self, clusterstrategy: ClusterStrategy) -> None:
        self._clusterstrategy = clusterstrategy

    def fit(self) -> pd.DataFrame:
        clustering = self._clusterstrategy.fit(self.corpus)
        return pd.DataFrame(
            clustering.labels_,
            # zip(corpus, clustering.labels_),
            columns=['Cluster_id'])
