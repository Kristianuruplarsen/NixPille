import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import random
import time
import urllib

def runItAll(begin, end, filename_main, filename_detail, images = False, save_merged = True):
    #scrape the main table on ecstasydata.org
    tableRead = table(begin, end).scrape(filename_main)
    tableData = pd.read_csv(filename_main +'.csv', encoding = 'latin1').drop_duplicates()
    print("Finished downloading main table")
    #scrape corresponding details
    detailRead = details.scrape(tableData['sampleLink'], filename_detail)
    detailData = pd.read_csv(filename_detail + '.csv', encoding = 'latin1')
    print('Finished downloading detail table - now merging')
    # merge on the sample link to add color (and other details?)
    mainData = pd.merge(tableData, detailData, on = 'sampleLink')
    if save_merged:
        mainData.to_csv('merged.csv', index = False)
    #download the corresponding images, if the user wishes
    if images:
        #create directory /images/ if it doesnt exist
        if not os.path.exists('./images/'):
            os.mkdir('./images/')
            print("created subfolder /images/")

        for i in range(0,len(mainData)):
            filename_img = mainData['imageLink'][i].split('/')[-1]
            #if image not in directory download it
            if not os.path.exists('./images/' + filename_img):
                images.download(mainData['imageLink'][i])
            else:
                print('file',filename_img,'already exists')
        print("Finished downloading images")
    return mainData


class table():
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end

    # scrape each page from begin to end and save as csv with name 'filename'
    def scrape(self, filename):
        outData = pd.DataFrame()
        linkList = self.makeLinkList(self.begin, self.end, 100)

        for i in range(0,len(linkList)):
            outData = outData.append(self.scrapeSinglePage(linkList[i]))
            # delay
            time.sleep(10 + random.uniform(-3,3))
            print("finished with run", i + 1 , "of", len(linkList))
        outData.to_csv(filename + '.csv', index = False)
        print("DONE")
        return outData


# make a list of links for scraping - can be used to build on an existing list
    def makeLinkList(self, viewsPrPage):
        linklist = []
        start = 0

        for i in range(self.begin, self.end):
            linklist.append('https://www.ecstasydata.org/index.php?sort=DatePublishedU+desc&start={}&search_field=-&m1=-1&m2=-1&datefield=tested&max={}&field_test=1'.format(start, viewsPrPage))
            start = start + viewsPrPage

        return linklist

    # get a page and process it with soupToData
    def scrapeSinglePage(link):
        resp = requests.get(link).text
        soup = BeautifulSoup(resp, 'html.parser')
        MainResults = soup.find(id = 'MainResults').tbody

        return table.soupToData(MainResults)


    # take the soup from scrapeSinglePage and convert it to a pandas dataframe
    # this could be reworked with some loops
    def soupToData(mainResult):
        #variable lists
        output = []

        prefix = 'https://www.ecstasydata.org'

        for row in mainResult.find_all('tr'):
            cells = row.find_all('td')
            outRow = {'img': [], 'smN_txt': [], 'smN_href': [], 'subst': [], 'amt': [], 'dateP':[], 'dateT': [],'loc': [], 'ss': [], 's': []}

            #get image, replace 'sm' with 'lg'
            try:
                outRow['img'].append(prefix + cells[0].find('img').get('src').replace('sm','lg'))
            except:
                outRow['img'] = None

            #sample name
            try:
                outRow['smN_txt'].append(cells[1].find('a').contents[0])
                outRow['smN_href'].append( '/'.join([prefix, cells[1].find('a').get('href')]))
            except:
                outRow['smN_txt'] = None
                outRow['smN_href'] = None

            #substance - requires some work to remove li tags
            for k, v in {'subst': 2, 'amt': 3}.items():
                try:
                    raw = cells[v].find_all('li')
                    for i in raw:
                        outRow[k].append(i.get_text())
                except:
                    outRow[k] = None

            for k, v in {'dateP': 4, 'dateT': 5, 'loc': 6, 'ss': 7}
                try:
                    outRow[k].append(cells[v].contents[0])
                except:
                    outRow[k] = None

            #dataSource
            try:
                outRow['s'].append(cells[8].find('a').contents[0])
            except:
                outRow['s'] = None

            R = []
            for key in outRow:
                R.append(outRow[key])

            output.append(R)
        output = pd.DataFrame(output)
        output.columns = ['imageLink', 'sampleName', 'sampleLink', 'substances', 'amounts', 'datePublished', 'dateTested','location', 'sampleSize', 'dataSource' ]
        return output



class images():
# download the images corresponding to each of the links supplied from table.scrape
    def download(linklist):
        for link in linklist:
            time.sleep(10 + random.uniform(-3,3))
            filename = link.split("/")[-1]
            resp = requests.get(link, stream = True)

            with open('./images/' + filename, 'wb') as f:
                for chunk in resp.iter_content(2000):
                    f.write(chunk)


class details():
#get the color from a single details page
    def scrapePage(link):
        resp = requests.get(link).text
        soup = BeautifulSoup(resp, 'html.parser')
        mainResult = soup.find_all('table')

        color = mainResult[2].find_all('td')[3].get_text()
        output = [link, color]
        return output

    def scrape(linklist, filename):
        out = []

        for i in range(0,len(linklist)):
            time.sleep(5 + random.uniform(-2,2))
            out.append(details.scrapePage(linklist[i]))
            print('Finished', i, 'of', len(linklist))
        #convert to pd dataframe and save to csv
        outData = pd.DataFrame(out)
        outData.columns = ['sampleLink', 'color']
        outData.to_csv(filename + '.csv', index = False)
        print("DONE")
        return outData

#runItAll(0,1,'main_t','detail_t')
