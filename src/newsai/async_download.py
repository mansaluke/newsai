__all__ = ["News", "HistoricalNews"]

import sys
from os import getcwd
from os.path import dirname, join, realpath, basename
from collections import defaultdict
import time
import typing
from json import load, loads
import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from bs4.element import Tag
import pandas as pd
from .utils.ipython import run_from_ipython
from .utils.nlogger import Log
from .newstypes import NewsDump, Url, Path, NewsDumpDict, StoryDict

log = Log("asyncio")

assert sys.version_info >= (3, 7), "Requirement: Python 3.7+."


class News():

    def __init__(self,
                 j_name: Path = 'url_config.json',
                 **kwargs: str):

        self.j_name = j_name
        self.find_futures_list = []
        self.responses = {}
        self.j_dict = self.filter_by_val(
            self.load_json(j_name), **kwargs
        )

    def __call__(self) -> list:
        if run_from_ipython:
            raise AttributeError(
                "If called iPython run_async() function should be called " +
                "directly. e.g. await News().run_async()")
        else:
            return self.run_async()

    def __repr__(self):
        return (f'{self.__class__.__name__}('
                f'config: {basename(self.j_name)})')

    @staticmethod
    def filter_by_val(j_dict: dict, **kwargs: str) -> dict:
        """
        we can pass in a list of arguments allowing
        us to filter the json by any values we like
        """
        def _filter_dict(j_dict, f_by, f_elem):
            return {k: v for k, v in j_dict.items()
                    if v[f_by] == f_elem}

        for k, v in kwargs.items():
            j_dict = _filter_dict(j_dict, k, v)

        return j_dict

    @staticmethod
    def load_json(j_name: dict) -> dict:
        if not run_from_ipython:
            json_path = join(
                dirname(realpath(__file__)), j_name
            )
        else:
            json_path = join(
                join(dirname(getcwd()), 'newsai'), j_name
            )
        with open(json_path) as f:
            return load(f)

    def add_futures(self, j_file: Path):
        for k, v in j_file.items():
            self.build_futures_get(v['url'])
            self.build_futures_search(int(k), v)

    def build_futures_get(self, url: Url):
        if url not in self.responses:
            self.responses[url] = asyncio.ensure_future(
                self.exec_get(
                    url=url)
            )

    def build_futures_search(self, config_id: int, url_info: dict):
        try:
            self.find_futures_list.append(
                asyncio.ensure_future(self.exec_find(
                    url_info["url"],
                    url_info["alias"],
                    url_info["name"],
                    url_info["cls_name"],
                    url_info["features"],
                    config_id
                )
                )
            )
        except KeyError:
            self.find_futures_list.append(
                asyncio.ensure_future(self.json_selector(
                    url_info["url"],
                    url_info["alias"],
                    url_info["json_key"],
                    config_id)
                )
            )

    def run_async(self) -> NewsDumpDict:
        self.add_futures(self.j_dict)
        loop = asyncio.get_event_loop()
        get_url_futures = asyncio.gather(
            *[f for f in self.responses.values()])
        find_text_futures = asyncio.gather(
            *[f for f in self.find_futures_list])

        final_future = asyncio.gather(get_url_futures, find_text_futures)

        if not run_from_ipython:
            loop.run_until_complete(final_future)
        else:
            asyncio.ensure_future(final_future)
        return NewsDump.story_dump

    async def exec_get(self, url: Url):
        log.debug(f'getting {url}')
        async with ClientSession() as session:
            self.responses[url] = await self.fetch_stories(
                session, url
            )

    async def exec_find(self, url: Url, alias: str,
                        name: str, cls_name: str,
                        features: str, config_id: str):
        await self.responses[url]
        await self.find_stories(
            url,
            alias,
            self.responses[url],
            name,
            cls_name,
            features,
            config_id
        )

    async def json_selector(self, url: Url, alias: str,
                            json_key: dict, config_id: int = 0):
        await self.responses[url]
        json_out = loads(self.responses[url])

        if isinstance(json_key['filter'], str):
            json_key['filter'] = [json_key['filter']]
        results = json_out
        for fltr in json_key['filter']:
            results = results[fltr]

        nd = NewsDump(config_id, url, alias)

        _stories = []
        for story in results:
            sd = StoryDict()
            for k, v in json_key['attribute'].items():
                if k in ('H0', 'H1', 'H2'):
                    val = story[v].encode(
                        'ascii', errors='ignore').decode('utf-8')
                else:
                    val = story[v]
                sd.update(**{k: val})
            nd.add_story(config_id, **sd)

    async def fetch_stories(self, session: ClientSession,
                            url: Url):
        async with session.get(url) as response:
            log.info(f"Status: {response.status}")
            return await response.text()

    @staticmethod
    async def find_stories(url: Url, alias: str,
                           response_text: str, name: str,
                           cls_name: str, features: str,
                           config_id: str = 0
                           ):
        """
        for websites w/o apis
        """
        nd = NewsDump(config_id, url, alias, name, cls_name, features)

        log.debug(f"searching url: {url}")
        soup = BeautifulSoup(response_text.encode(
            'ascii', errors='ignore').decode('utf-8'), features=features)
        _stories = []

        def _return_sibling(tag: Tag):
            try:
                return tag.text
            except AttributeError:
                return None

        if type(cls_name) == str:
            cls_name = [cls_name]

        for c in cls_name:
            element = soup.find_all(name, {"class": c})
            if len(element) == 0:
                log.warning('No output returned for id:' +
                            '{0}, cls_name: {1}, url: {2}'.format(
                                config_id, cls_name, url))
            for i in element:
                nd.add_story(config_id,
                             _return_sibling(i.previousSibling),
                             _return_sibling(i),
                             _return_sibling(i.nextSibling)
                             )


class HistoricalNews(News):
    def __init__(self,
                 year:  int,
                 month: int,
                 j_name: Path = 'url_hist_config.json',
                 **kwargs: str):
        super().__init__(j_name=j_name, **kwargs)
        self.year = year
        self.month = month
        for element in self.j_dict.values():
            element["url"] = element["url"].format(year, month)

    def __call__(self) -> list:
        return self.run_async()

    def __repr__(self):
        return (f'{self.__class__.__name__}('
                f'config: {basename(self.j_name)}, '
                f'year: {self.year}, month: {self.month})')
