import requests
from bs4 import BeautifulSoup
from termcolor import colored
from pandas import DataFrame
import pandas as pd


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
    stories[['flag_x', 'flag_y']] = stories[['flag_x', 'flag_y']].fillna(0)
    stories['flag'] = stories['flag_x'] + stories['flag_y']
    stories = stories.drop(['flag_x', 'flag_y'], axis=1)
    return stories


       



if __name__ == "__main__":
    url = 'https://www.bbc.com/news'
    soup = get(url)
    all_stories = find_all_stories(soup)
    #print(all_stories.head())

