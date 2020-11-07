import unittest
import asyncio
from newsai.async_download import (News, HistoricalNews, Url, ClientSession)


async def test_fetch_stories(self, session: ClientSession,
                             url: Url):
    async with session.get(url) as response:
        return response.status


class TestNews(unittest.TestCase):

    def test_url_status(self) -> list:
        News.fetch_stories = test_fetch_stories
        current_news = News()

        HistoricalNews.fetch_stories = test_fetch_stories
        hist_news = HistoricalNews(year=2020, month=1)

        for news in (current_news, hist_news):
            for source in news.j_dict.values():
                news.build_futures_get(source['url'])

            loop = asyncio.get_event_loop()
            get_url_futures = asyncio.gather(
                *[ftr for ftr in news.responses.values()])

            loop.run_until_complete(
                get_url_futures
            )

            for url, status in news.responses.items():
                print(f'{url}: {status}')
                self.assertEqual(status, 200)


# python -m unittest tests.test_async_download
