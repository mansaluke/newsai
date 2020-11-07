import unittest
from newsai.newstypes import NewsDump, StoryDict, ndict


class test_NewsDump(unittest.TestCase):

    def test_StoryDict(self):
        story_dict = StoryDict('a', 'b', 'c')
        assert list(story_dict.keys()) == ['H0', 'H1', 'H2']

    def test_ndict(self):
        news_dct = ndict('Story a', 'Story a details\n\n\nStory b')
        assert isinstance(news_dct, list)
        assert len(news_dct) == 2
        assert isinstance(news_dct[0], StoryDict)
        assert len(news_dct[0]) == 2
        assert list(news_dct[0].values()) == ['Story a', 'Story a details']
        assert list(news_dct[1].values()) == ['Story b']

    def test_add_story(self):
        news_dump = NewsDump(1, 22, 3, 4, 5, 6)
        news_dump = NewsDump(2, 22, 3, 4, 5, 6)
        news_dump.add_story(1, 'hi', 'hello')
        news_dump.add_story(2, 'a story', 'a good story')
        news_dump.add_story(2, 'a story', 'a bad story')
        news_dump.add_story(2, 'a story', 'a good story')
        news_dump.add_story(1, 'hi', 'another')
        assert len(news_dump) == 2


# python -m unittest tests.test_newstypes
