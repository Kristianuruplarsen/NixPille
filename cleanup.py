import scraper
import pandas as pd

## be aware that downloading images will get a LOT of jpg's
#runItAll(0,53,'main_t','detail_t', images = True)

##
data = pd.read_csv('merged.csv', encoding = 'latin1', parse_dates = ['datePublished', 'dateTested'])

pd.to_datetime(data['datePublished'])

data['mg_dum'] = data['amounts']

data

data.duplicated(['sampleName'])
