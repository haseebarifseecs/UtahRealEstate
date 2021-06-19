import mysql.connector
import requests
from bs4 import BeautifulSoup
import re

'''
connection = ''
def con():
    try:
        connection = mysql.connector.connect(host="localhost",port="3306",user="root",passwd="")
        return connection.cursor()
    except Exception as e:
        print("[+] Failed to connect\nPrinting error stack\n" + str(e))


def execute_query():
    conn = con()
    try:
        conn.execute("SELECT mls_id FROM properties.entries WHERE status = 'active'")
        if conn:
            for mls_id in conn:
                print("https://utahrealestate.com/" + str(mls_id[0]))
        else:
            pass

    except Exception as e:
        print("Error Occured\n Printing error stack\n" + str(e))


execute_query()
'''

class scraper:
    completeResult = []
    urls = []
    globalData = dict()
    agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36'
    price = ''
    agent_name = ''
    broker_name = ''
    broker_address = ''
    broker_csz = ''
    beds = ''
    baths = ''
    sqft = ''
    agent_phone = ''
    broker_logo_url = ''
    property_description = ''
    mls_status = ''
    agent_pic_url = ''
    def __init__(self,host, port, user, passwd):
        self.connection = mysql.connector.connect(host=host,port=port,user=user,passwd=passwd)
        self.cur = self.connection.cursor()

    def executeQuery(self,SQL):
        try:
            self.cur.execute(SQL)
            #print(self.cur)
            #print("testing")
            if self.cur:
                for mls_id in self.cur:
                    url = "https://utahrealestate.com/" + str(mls_id[0])
                    self.urls.append({'url':url, 'mls_id':str(mls_id[0])})
                    #print("https://utahrealestate.com/" + str(mls_id[0]))
            
            else:
                pass
            
        except Exception as e:
            print("Error occured \n printing error stack\n" + str(e))
        print(self.urls)



    def addResults(self):
        for info in self.completeResult:
            price = info.get('price')
            agent_name = info.get('agent_name')
            broker_name = info.get('broker_name')
            broker_address = info.get('broker_address')
            broker_csz = info.get('broker_csz')
            beds = info.get('beds')
            baths = info.get('baths')
            sqft = info.get('sqft')
            agent_phone = info.get('agent_phone')
            broker_logo_url = info.get('broker_logo_url')
            property_description = info.get('property_description')
            mls_status = info.get('mls_status')
            agent_pic_url = info.get('agent_pic_url')
            mls_id = info.get('mls_id')
            if not price:
                price = "null"
            else:
                price = price.strip()
            if not agent_name:
                agent_name = "null"
            else:
                agent_name = agent_name.strip()
            if not broker_name:
                broker_name = "null"
            else:
                broker_name = broker_name.strip()
            if not broker_address:
                broker_address = "null"
            else:
                broker_address = broker_address.strip()
            if not broker_csz:
                broker_csz = "null"
            else:
                broker_csz = broker_csz.strip()
            if not beds:
                beds = "null"
            else:
                beds = beds.strip()
            if not baths:
                baths="null"
            else:
                baths = baths.strip()
            if not sqft:
                sqft = "null"
            else:
                sqft = sqft.strip()
            if not agent_phone:
                agent_phone = "null"
            else:
                agent_phone = agent_phone.strip()
            if not broker_logo_url:
                broker_logo_url = "null"
            if not property_description:
                property_description = "null"
            else:
                property_description = property_description.strip().replace('"','')
            if not mls_status:
                mls_status = "null"
            else:
                mls_status = mls_status.strip()
            if not agent_pic_url:
                agent_pic_url = "null"
            
            SQL = f'UPDATE properties.entries SET price = "{price}", agent_name = "{agent_name}", broker_name = "{broker_name}", broker_address = "{broker_address}", broker_csz = "{broker_csz}", beds = "{beds}", baths = "{baths}", sqft = "{sqft}", agent_phone = "{agent_phone}", broker_logo_url = "{broker_logo_url}", property_description = "{property_description}", mls_status = "{mls_status}", agent_pic_url = "{agent_pic_url}" WHERE mls_id = {mls_id}'
            #print(SQL)
            self.cur.execute(SQL)
        self.connection.commit()

    def scrapeData(self):
        for url in self.urls:
            #print(url.get('url'))
            data = requests.get(url.get('url'),headers={'user-agent':self.agent}).content
            soup = BeautifulSoup(data,'lxml')
            self.price = soup.select('#prop-details > div.container > div > div.col-xs-12.col-sm-6.col-md-6.col-lg-4.border-top > div > div:nth-child(1) > span')
            self.agent_name = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-4.col-lg-4 > div > div.agent-overview.clear > div > div > div.agent-overview-content')
            self.broker_name = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-4.col-lg-4 > div > div.broker-overview.clear > div > div')
            self.beds = soup.select('#prop-details > div.container > div > div.col-xs-12.col-sm-6.col-md-6.col-lg-4.border-top > div > div:nth-child(2) > span')
            self.baths = soup.select('#prop-details > div.container > div > div.col-xs-12.col-sm-6.col-md-6.col-lg-4.border-top > div > div:nth-child(3) > span')
            self.sqft = soup.select('#prop-details > div.container > div > div.col-xs-12.col-sm-6.col-md-6.col-lg-4.border-top > div > div:nth-child(4) > span')
            self.broker_logo_url = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-4.col-lg-4 > div > div.broker-overview.clear > div > div > img')
            self.property_description = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-8.col-lg-8.prop-overview-wrap > div.features-wrap')
            self.mls_status = soup.select('body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-8.col-lg-8.prop-overview-wrap > div.row.facts > div:nth-child(2) > div > div.fact-copy-wrap ')
            self.agent_pic_url = soup.select("body > section:nth-child(3) > div > div.row > div.col-xs-12.col-md-4.col-lg-4 > div > div.agent-overview.clear > div:nth-child(2) > div > div.agent-overview-photo > div > img")

            if self.price:
                self.price = self.price[0].text.strip()
                self.price = self.price.replace("$","").replace(",","")
                self.globalData['price'] = self.price
            if self.agent_name:
                self.globalData['agent_name'] =  (self.agent_name[0].text.split('\n'))[1]
                self.globalData['agent_phone'] = (self.agent_name[0].text.split('\n'))[2].strip()

            if self.broker_name:
                self.broker_name = self.broker_name[0].text.strip()
                self.broker_name = self.broker_name.split("\n")
                self.globalData['broker_name'] = self.broker_name[0].strip()
                self.globalData['broker_address'] = (self.broker_name[1] + self.broker_name[2]).strip()
                self.globalData['broker_csv'] = self.broker_name[3].strip()

            if self.beds:
               self.beds = self.beds[0].text.strip()
               self.globalData['beds'] = self.beds

            if self.baths:
                self.baths = self.baths[0].text.strip()
                self.globalData['baths'] = self.baths

            if self.sqft:
                self.sqft = self.sqft[0].text.strip()
                self.globalData['sqft'] = self.sqft

            if self.broker_logo_url:
                self.broker_logo_url = self.broker_logo_url[0]['src']
                if 'http:' not in self.broker_logo_url:
                    self.broker_logo_url = 'https:' + self.broker_logo_url
                self.globalData['broker_logo_url'] = self.broker_logo_url

            if self.property_description:
                self.property_description = self.property_description[0].text.strip()
                self.globalData['property_description'] = self.property_description.replace("\n","")

            if self.mls_status:
                self.mls_status = self.mls_status[0].text.strip()
                self.mls_status = self.mls_status.split("\n")[1].strip()
                self.globalData['mls_status'] = self.mls_status

            if self.agent_pic_url:
                self.agent_pic_url = self.agent_pic_url[0]['src']
                if 'http:' not in self.agent_pic_url:
                    self.agent_pic_url = 'https:' + self.agent_pic_url
                self.globalData['agent_pic_url'] = self.agent_pic_url
            self.globalData['mls_id'] = url.get('mls_id')    
            #print(self.globalData)
            self.completeResult.append(self.globalData)
            self.addResults()
            
            
                                     



    def closeCon(self):
        self.cur.close()
        self.connection.close()
        
    
                                     

                    


sscraper = scraper("URL","PORT","USERNAME","PASSWORD")
sscraper.executeQuery("SELECT mls_id FROM properties.entries WHERE status = 'active' AND mls_id != ''")
sscraper.scrapeData()
sscraper.closeCon()
