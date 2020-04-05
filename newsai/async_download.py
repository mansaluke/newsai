import sys
import os
from json import load, loads
import asyncio
import aiofiles
# import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from bs4.element import Tag
import pandas as pd
from datetime import datetime
import time
from dfconvert import df_store

assert sys.version_info >= (3, 7), "Script requires Python 3.7+."


class News():

    def __init__(self, j_name='uri_config.json', **kwargs):
        self.find_futures_list = []
        self.responses = {}
        self.j_dict = self.filter_by_val(
                self.load_json(j_name), **kwargs
                    )

    def __call__(self):
        self.add_futures(
            self.j_dict
        )
        return self.run_async()

    @staticmethod
    def filter_by_val(j_dict, **kwargs):
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
    def load_json(j_name) -> dict:
        json_path = os.path.join(
            os.getcwd(), 'newsai', j_name
        )
        with open(json_path) as f:
            return load(f)

    def add_futures(self, j_file):

        for v in j_file.values():
            self.build_futures_get(v['uri'])
            self.build_futures_search(v)

    def build_futures_get(self, uri):
        if uri not in self.responses:
            self.responses[uri] = asyncio.ensure_future(
                self.exec_get(
                    uri=uri)
            )

    def build_futures_search(self, uri_info: dict):
        try:
            self.find_futures_list.append(
                asyncio.ensure_future(self.exec_find(
                    uri_info["uri"],
                    uri_info["alias"],
                    uri_info["name"],
                    uri_info["cls_name"],
                    uri_info["features"]
                )
                )
            )
        except KeyError:
            self.find_futures_list.append(
                asyncio.ensure_future(self.json_selector(
                    uri_info["uri"],
                    uri_info["alias"],
                    uri_info["json_key"])
                )
            )
            # print(f'key error for {uri_info["uri"]}')

    def run_async(self):
        loop = asyncio.get_event_loop()
        get_url_futures = asyncio.gather(
            *[f for f in self.responses.values()])
        find_text_futures = asyncio.gather(
            *[f for f in self.find_futures_list])

        return loop.run_until_complete(
            asyncio.gather(get_url_futures, find_text_futures)
        )[1]

    async def exec_get(self, uri):
        print(f'getting {uri}')
        async with ClientSession() as session:
            self.responses[uri] = await self.fetch_stories(
                session, uri
            )
        return None

    async def exec_find(self, uri, alias, name, cls_name, features):
        await self.responses[uri]
        stories_out = await self.find_stories(
            uri,
            alias,
            self.responses[uri],
            name,
            cls_name,
            features
        )
        return stories_out

    async def json_selector(self, uri, alias, json_key):
        await self.responses[uri]
        json_out = loads(self.responses[uri])
        results = json_out[json_key['filter']]

        _stories = []
        for story in results:
            main_stories_dict = {}
            for k, v in json_key['attribute'].items():
                main_stories_dict.update({k: story[v]})
            main_stories_dict.update({
                'uri': uri,
                'date': datetime.now()
            })
            _stories.append(main_stories_dict)
        return _stories

    async def fetch_stories(self, session, url):
        async with session.get(url) as response:
            print("Status: ", response.status)
            return await response.text()

    @staticmethod
    async def find_stories(uri, alias, response_text, name, cls_name, features=None):
        """
        for websites w/o apis
        """
        print(f'searching uri: {uri}')
        soup = BeautifulSoup(response_text, features=features)
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
                    'alias': alias,
                    'uri': uri,
                    'date': datetime.now()
                }

                if len(main_stories_dict) > 0:
                    _stories.append(main_stories_dict)
                else:
                    print(f'warning: dict empty for uri: {uri}')
        return _stories


if __name__ == "__main__":
    # initiating
    m = News()
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
    file_name = 'all_stories_async.csv'

    try:
        df_store(file_name).store_df(df)
    except FileExistsError:
        df_store(file_name).append_df(df)
