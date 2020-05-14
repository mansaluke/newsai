__all__ = ["News", "HistoricalNews"]

import sys
from os import getcwd
from os.path import dirname, join, realpath
import time
import typing
import pathlib
from json import load, loads
import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from bs4.element import Tag
from datetime import datetime
import pandas as pd
from .utils.ipython import run_from_ipython
from .utils.nlogger import Log

log = Log("asyncio")

assert sys.version_info >= (3, 7), "Requirement: Python 3.7+."


Url = str
Path = typing.Union[str, pathlib.Path]


class News():

    def __init__(self,
                 j_name: Path = 'url_config.json',
                 **kwargs: str):

        self.find_futures_list = []
        self.responses = {}
        self.j_dict = self.filter_by_val(
            self.load_json(j_name), **kwargs
        )

    def __call__(self) -> list:
        if run_from_ipython:
            raise AttributeError(
                "If called iPython run_async() function should be called directly. e.g. await News().run_async()")
        else:
            return self.run_async()[1]

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
        for v in j_file.values():
            self.build_futures_get(v['url'])
            self.build_futures_search(v)

    def build_futures_get(self, url: Url):
        if url not in self.responses:
            self.responses[url] = asyncio.ensure_future(
                self.exec_get(
                    url=url)
            )

    def build_futures_search(self, url_info: dict):
        try:
            self.find_futures_list.append(
                asyncio.ensure_future(self.exec_find(
                    url_info["url"],
                    url_info["alias"],
                    url_info["name"],
                    url_info["cls_name"],
                    url_info["features"]
                )
                )
            )
        except KeyError:
            self.find_futures_list.append(
                asyncio.ensure_future(self.json_selector(
                    url_info["url"],
                    url_info["alias"],
                    url_info["json_key"])
                )
            )

    def run_async(self) -> list:
        self.add_futures(
            self.j_dict
        )
        loop = asyncio.get_event_loop()
        get_url_futures = asyncio.gather(
            *[f for f in self.responses.values()])
        find_text_futures = asyncio.gather(
            *[f for f in self.find_futures_list])

        final_future = asyncio.gather(get_url_futures, find_text_futures)

        if not run_from_ipython:
            return loop.run_until_complete(
                final_future
            )
        else:
            return asyncio.ensure_future(
                final_future)

    async def exec_get(self, url: Url):
        log.debug(f'getting {url}')
        async with ClientSession() as session:
            self.responses[url] = await self.fetch_stories(
                session, url
            )

    async def exec_find(self, url: Url, alias: str,
                        name: str, cls_name: str,
                        features: str) -> list:
        await self.responses[url]
        stories_out = await self.find_stories(
            url,
            alias,
            self.responses[url],
            name,
            cls_name,
            features
        )
        return stories_out

    async def json_selector(self, url: Url, alias: str,
                            json_key: dict) -> list:
        await self.responses[url]
        json_out = loads(self.responses[url])

        if isinstance(json_key['filter'], str):
            json_key['filter'] = [json_key['filter']]
        results = json_out
        for fltr in json_key['filter']:
            results = results[fltr]

        _stories = []
        for story in results:
            main_stories_dict = {}
            for k, v in json_key['attribute'].items():
                story_clean = story[v].encode(
                    'ascii', errors='ignore').decode('utf-8')
                main_stories_dict.update(
                    {k: story_clean})
            main_stories_dict.update({
                'alias': alias,
                'url': url
            })
            _stories.append(main_stories_dict)
        return _stories

    async def fetch_stories(self, session: ClientSession,
                            url: Url):
        async with session.get(url) as response:
            log.info(f"Status: {response.status}")
            return await response.text()

    @staticmethod
    async def find_stories(url: Url, alias: str,
                           response_text: str, name: str,
                           cls_name: str, features: str
                           ) -> list:
        """
        for websites w/o apis
        """
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
            for i in soup.find_all(name, {"class": c}):
                main_stories_dict = {
                    'H0': _return_sibling(i.previousSibling),
                    'H1': _return_sibling(i),
                    'H2': _return_sibling(i.nextSibling),
                    'date': datetime.now(),
                    'alias': alias,
                    'url': url
                }

                if len(main_stories_dict) > 0:
                    _stories.append(main_stories_dict)
                else:
                    log.warning(f'Empty dictionary returned for url: {url}')
        return _stories


class HistoricalNews(News):
    def __init__(self,
                 year:  int,
                 month: int,
                 j_name: Path = 'url_hist_config.json',
                 **kwargs: str):
        super().__init__(j_name=j_name, **kwargs)
        for element in self.j_dict.values():
            element["url"] = element["url"].format(year, month)

    def __call__(self) -> list:
        return self.run_async()[1]
