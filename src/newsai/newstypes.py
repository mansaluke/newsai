from collections import defaultdict
from datetime import datetime
import typing
from typing import Union, Optional, List
import pathlib
import re
import pandas as pd
from newsai.utils.nlp import split_on_uppercase

__all__ = ["Url", "Path", "NewsDumpDict",
           "StoryHolderDict", "StoryDict", "Singleton", "NewsDump"]

Url = str
Path = Union[str, pathlib.Path]


class NewsDumpDict(dict):
    def __repr__(self):
        return "{" + "\n".join("{!r}: {!r},".format(k, v)
                               for k, v in self.items()) + "}"


class StoryHolderDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __repr__(self):
        r = ""
        for k, v in self.items():
            if k == "stories":
                r = r + "".join("{!r}:\n{!r}".format(k, v))
            else:
                r = r + "".join("{!r}: {!r},".format(k, v))
        return r

    def to_pandas(self, cols=None) -> pd.DataFrame:
        df = pd.DataFrame(self['stories'])
        if cols is None:
            cols = ('datetime', 'url', 'alias')
        for k, v in self.items():
            if k in cols:
                df[k] = v
        return df


class StoryDict(defaultdict):
    def __init__(self, *args, **kwargs):
        super().__init__(lambda: None, **kwargs)

        new_elements = [a for a in args if a is not None]

        for i in range(len(new_elements)):
            self.__setitem__('H' + str(i), new_elements[i])

    def __repr__(self):
        return str(dict(self))


class Ndict(object):
    def __new__(cls, *args, **kwargs) -> Optional[List]:
        if kwargs:
            out = [StoryDict(**kwargs)]
        else:
            in_story = StoryDict(*args, **kwargs)
            in_story_vals = list(in_story.values())
            split_args = []
            max_len = 1
            for i in in_story_vals:
                sp = re.split('\n+', i)
                spl = []
                for i in sp:
                    spl.extend(split_on_uppercase(i))
                num_lines = len(spl)
                split_args.append(spl)
                if num_lines > max_len:
                    max_len = num_lines
            expanded_list = []
            for i in range(max_len):
                values_at_level = []
                for j in split_args:
                    try:
                        values_at_level.append(j[i])
                    except IndexError:
                        pass
                expanded_list.append(values_at_level)
            out = []
            for i in expanded_list:
                if any(i):
                    out.append(StoryDict(*i))
        if any(out):
            return out
        else:
            return None


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]


class NewsDump(metaclass=Singleton):
    """
    replace current strucure:
    list of default dicts
    dict_keys(['id', 'datetime', 'url', 'alias', 'name',
              'cls_name', 'features', 'stories'])
    stories is a list of dictionaries each like the following
    {'H0': 'LiveLiveUS passes two million cases as states report rise',
        'H1': 'Daily infections are still rising in some states as governments ease restrictions.', 'H2': None}
    """

    story_dump = NewsDumpDict()

    def __init__(self,
                 config_id: str,
                 url: str,
                 alias: Optional[str] = None,
                 name: Optional[str] = None,
                 cls_name: Optional[str] = None,
                 features: Optional[str] = None):
        self.story_dump[config_id] = StoryHolderDict(
            datetime=datetime.now(),
            url=url,
            alias=alias,
            name=name,
            cls_name=cls_name,
            features=features,
            stories=[])

    def __repr__(self):
        return (f'{self.__class__.__name__}')

    def __len__(self):
        return len(NewsDump.story_dump)

    def add_story(self, config_id, *args, **kwargs):
        to_append = self._get_story_dict(*args, **kwargs)
        if to_append:
            for i in to_append:
                NewsDump.story_dump[config_id]['stories'].append(i)

    def _get_story_dict(self, *args, **kwargs) -> Optional[List]:
        return Ndict(*args, **kwargs)
