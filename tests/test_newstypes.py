import unittest
from newsai.newstypes import NewsDump, StoryDict, ndict


class test_NewsDump(unittest.TestCase):

    def test_StoryDict(self):
        s = StoryDict('a', 'b', 'c')
        assert list(s.keys()) == ['H0', 'H1', 'H2']

    def test_ndict(self):
        n = ndict('Story a',
                  'Story a details\n\n\nStory b')
        assert isinstance(n, list)
        assert len(n) == 2
        assert isinstance(n[0], StoryDict)
        assert len(n[0]) == 2
        assert list(n[0].values()) == ['Story a', 'Story a details']
        assert list(n[1].values()) == ['Story b']

    def test_add_story(self):
        n = NewsDump(1, 22, 3, 4, 5, 6)
        n = NewsDump(2, 22, 3, 4, 5, 6)
        n.add_story(1, 'hi', 'hello')
        n.add_story(2, 'a story', 'a good story')
        n.add_story(2, 'a story', 'a bad story')
        n.add_story(2, 'a story', 'a good story')
        n.add_story(1, 'hi', 'another')
        assert len(n) == 2


# python -m unittest tests.test_newstypes
