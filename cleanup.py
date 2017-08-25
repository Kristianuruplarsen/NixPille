import scraper
import pandas as pd

## be aware that downloading images will get a LOT of jpg's
scraper.runItAll(0,2,'main_t','detail_t')

##
data = pd.read_csv('merged.csv', encoding = 'latin1', parse_dates = ['datePublished', 'dateTested'])

pd.to_datetime(data['datePublished', 'dateTested'])
