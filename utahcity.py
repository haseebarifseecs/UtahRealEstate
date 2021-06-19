import requests
import json
import re
from bs4 import BeautifulSoup
import csv
from datetime import date
from threading import Thread
DATA = []
MLS_ID = []
today = date.today()
D = today.strftime("%b%d%Y")
def mainScraper():
    url = "https://utahrealestate.com/search/chained.update/count/false/criteria/false/pg/1/limit/50/dh/807";
    parameters = {"param":"city","value":"Salt Lake","param_reset":"county_code,o_county_code,city,o_city,zip,o_zip,geometry,o_geometry","chain":"saveLocation,criteriaAndCountAction,mapInlineResultsAction","tx":"true","all":"1","accuracy":"4","geocoded":"Salt Lake","state":"UT","htype":"city","geolocation":"salt lake city","o_env_certification":"32"}
    data = requests.post(url, data=parameters).json()

    if data:
        try:
            listingData = data['markers']
            if listingData:
                for l in listingData:
                    #print(l.get('id', 'missing'))
                    MLS_ID.append(l.get('id','missing'))
            else:
                pass
        except:
            pass
    return MLS_ID

def scraper(id):
    info = dict()
    hasTour = 'No'
    agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36'
    url = "https://www.utahrealestate.com/" + id
    data = requests.get(url,headers={'user-agent':agent}).content
    soup = BeautifulSoup(data,'lxml')
    address_one = soup.select('#prop-details > div.container > div > div:nth-child(1) > div > a > div > div:nth-child(2) > div > span > h2')
    address_two = soup.select('#prop-details > div.container > div > div:nth-child(1) > div > a > div > div:nth-child(2) > div > span > p')
    brokerage = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-4.col-lg-4 > div > div.broker-overview.clear > div > div > strong')
    price = soup.select('#prop-details > div.container > div > div.col-xs-12.col-sm-6.col-md-6.col-lg-4.border-top > div > div:nth-child(1) > span')
    beds = soup.select('#prop-details > div.container > div > div.col-xs-12.col-sm-6.col-md-6.col-lg-4.border-top > div > div:nth-child(2) > span')
    bathrooms = soup.select('#prop-details > div.container > div > div.col-xs-12.col-sm-6.col-md-6.col-lg-4.border-top > div > div:nth-child(3) > span')
    sqft = soup.select('#prop-details > div.container > div > div.col-xs-12.col-sm-6.col-md-6.col-lg-4.border-top > div > div:nth-child(4) > span')
    tour = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-8.col-lg-8.prop-overview-wrap > div.share-wrap.clear.no-print > div > div:nth-child(1) > a')
    ureDays = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-8.col-lg-8.prop-overview-wrap > div.row.facts > div:nth-child(1) > div > div.fact-copy-wrap')
    agentInfo = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-4.col-lg-4 > div > div.agent-overview.clear > div > div > div.agent-overview-content')

    if address_one and address_two:
        address_one = address_one[0].text.strip()
        address_two = address_two[0].text.strip()
        address_one = address_one.replace("\n","")
        address_two = address_two.replace("\n","")
        postal = address_two.split(',')[1]
        address = address_one + " " + address_two[:address_two.find(',')]
        info['address'] = address
        info['postal'] = postal
    if brokerage:
        brokerage = brokerage[0].text.strip()
        info['brokerage'] = brokerage
    if price:
        price = price[0].text.strip()
        price = price.replace("$","").replace(",","")
        info['price'] = int(price)
    if beds:
        beds = beds[0].text.strip()
        info['beds'] = beds
    if bathrooms:
        bathrooms = bathrooms[0].text.strip()
        info['bathrooms'] = bathrooms
    if sqft:
        sqft = sqft[0].text.strip()
        info['sqft'] = sqft
    if tour:
        hasTour = "Yes"
        info['hasTour'] = hasTour
        link = re.compile("'http(.+?)'",re.DOTALL).findall(str(tour[0]))
        if link:
            link = link[0]
            link = 'http' + link
            info['tourLink'] = link
            domain = link[:link.find('.com')] + '.com'
            domain = domain.replace("http://","")
            info['tourDomain'] = domain
    else:
        info['hasTour'] = hasTour
        info['tourLink'] = ''
        info['tourDomain'] = ''
    if ureDays:
        ureDays = ureDays[0].text.split('\n')[2].strip()
        info['ureDays'] = ureDays
    if agentInfo:
        agentInfo = agentInfo[0].text.split('\n')
        agentName = agentInfo[1]
        contactNo = agentInfo[2]
        info['agentName'] = agentName
        info['contactNo'] = contactNo.strip()
    info['mlsid'] = id

    print(info)
    DATA.append(info)
    return

threadlist = []
def fastScraper(index, id):
    i = (index) * (len(id) // 8)
    j = (index + 1) * (len(id) //8)
    while i < j:
        scraper(id[i])
        i+=1


ids = mainScraper()
array_size = len(ids)

for i in range(8):
    t = Thread(target=fastScraper, args=(i,ids))
    t.start()
    threadlist.append(t)
    

for waitt in threadlist:
    waitt.join()



csv_file ="URE_SaltLakeCounty_" + D + ".csv"
csv_columns = ['address','postal', 'brokerage', 'price', 'beds', 'bathrooms', 'sqft', 'hasTour', 'tourLink', 'tourDomain', 'ureDays', 'agentName', 'contactNo', 'mlsid']
try:
    with open(csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in DATA:
            writer.writerow(data)
except IOError:
    print("I/O error") 

