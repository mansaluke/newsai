import requests
from bs4 import BeautifulSoup
from termcolor import colored
from pandas import DataFrame
import pandas as pd
from datetime import datetime

from dfconvert import df_store

def get(url):

    response = requests.get(url)

    if response.status_code == 200:
        print('success')
    elif response.status_code == 404:
        print('not found')

    return BeautifulSoup(response.text)


def find_all_stories(soup):
    """
    finds all stories (headers and bodies of text) 
    flag classifies story 'importance':
        1 = top story
        0 = secondary story
        -1 = other story
    """

    cls_search_headmain ="gs-c-promo-heading gs-o-faux-block-link__overlay-link gel-paragon-bold nw-o-link-split__anchor" 
    cls_search_head =  "gs-c-promo-heading gs-o-faux-block-link__overlay-link gel-pica-bold nw-o-link-split__anchor"
    cls_search_body = "gs-c-promo-summary gel-long-primer gs-u-mt nw-c-promo-summary"


    def find_stories(name, cls_name, flag):
        _stories = []
        for i in soup.find_all(name, {"class": cls_name}):
            try:
                h1 = i.previousSibling.text
            except:
                h1 = None
            try:
                h2 = i.text
            except:
                h2 = None

            if flag == 1:
                main_stories_dict = {'header':h1,'body':h2, 'flag': flag}
            elif flag == -1:
                main_stories_dict = {'header':h2, 'flag': flag}

            _stories.append(main_stories_dict)

        return DataFrame(_stories)
    
    main_stories = find_stories("p", cls_search_body, 1)
    sub_stories = find_stories("a", cls_search_head, -1)

    stories = pd.merge(main_stories, sub_stories, on = ['header', 'header'], how='outer')
    stories = stories.drop_duplicates()

    stories[['flag_x', 'flag_y']] = stories[['flag_x', 'flag_y']].fillna(0)
    stories['flag'] = stories['flag_x'] + stories['flag_y']
    stories = stories.drop(['flag_x', 'flag_y'], axis=1)
    stories['flag'] = stories['flag'].astype(int)

    return stories





if __name__ == "__main__":

    url = 'https://www.bbc.com/news'
    filename = 'all_stories.h5'

    try:
        df = df_store(filename).load_df()
        len_df0 = len(df)
    except:
        raise IOError('could not load file')
    
    soup = get(url)
    all_stories = find_all_stories(soup)

    current_date = datetime.now()

    all_stories['url'] = url
    all_stories['date'] = current_date


    #df_store('all_stories_'+ str(current_date.strftime("%Y%m%d_%H%M%S")) + '.csv').store_df(all_stories)
    
    try:
        df_store(filename).store_df(all_stories)
    except:
        df_store(filename).append_df(all_stories)
    df = df_store(filename).load_df()
    len_df1 = len(df)
    print(df)
    print(len(df))

    try:
        print('{} lines added'.format(len_df1 - len_df0))
    except:
        pass