This library is made of two parts:
- ``Extraction`` (STABLE) (requires Python 3.7+) - see [examples](https://github.com/mansaluke/newsai/tree/master/examples) and [sample data](https://github.com/mansaluke/newsai/tree/master/data) for both real-time and historical data.
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

Historical news data from:
- NYTimes


To use your own data sources you can update the json config file or provide your own.


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

(WIP)
