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
        n = News()
        for v in n.j_dict.values():
            n.build_futures_get(v['url'])

        loop = asyncio.get_event_loop()
        get_url_futures = asyncio.gather(
            *[f for f in n.responses.values()])

        loop.run_until_complete(
            get_url_futures
        )

        for url, status in n.responses.items():
            print(f'{url}: {status}')
            self.assertEqual(status, 200)


if __name__ == '__main__':
    unittest.main()
