This library is made of two parts:
- ``Extraction`` (STABLE) (requires Python 3.7+)
- ``Analysis`` (WIP)

#### News api download sources
Real-time data sources incorporated:
- BBC News
- FOX News
- ABC News
- NYTimes
- YahooFinance
- Sky News
- Forbes
- CNBC Finance

And it should be use your own data sources using a json config file.

Historical news data from:
- NYTimes

Other news sources:
 - https://github.com/abisee/cnn-dailymail processes the https://cs.nyu.edu/~kcho/DMQA/ DeepMind Dataset (2015 - CNN & DailyMail)
 - Kaggle million-headlines dataset (ABC News)
 - GLUE & SUPERGLUE RTE datasets

#### Usage
``` cmd
python examples/get_historical_news_data.py
python examples/get_news_data.py
```

#### Analysis (NLP)

Run nlp examples from colab for an idea of whats to come.

Basic: [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mansaluke/newsai/blob/master/notebooks/nlp_basics.ipynb)


Advanced: [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mansaluke/newsai/blob/master/notebooks/nlp_advanced.ipynb)

