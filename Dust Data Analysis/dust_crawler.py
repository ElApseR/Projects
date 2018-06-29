import requests
import pandas as pd
import numpy as np
import googlemaps
import datetime
from bs4 import BeautifulSoup as bs

class crawler():
    '''Crawls information about station, measure. And preprocess for analysis.
    
    Methods
    -------
    _station_crawler : Gather information about stations.
    _preprocessor : Preprocess data about station and measure.
    _dust_realtime_crawler : Gather realtime dust measure
    _dust_history_crawler : Gather dust measure from each stations.
    
    Args
    ----
    term : required term. (1day='DAILY', 1month='MONTH', 3month='3MONTH')
    pageNo : num of page. 
    numOfRows : num of rows in a page
    key_on_use : If over traffic, use another key(0,1)
    
    '''
    def __init__(self):
        self.term = '3MONTH'
        self.pageNo = '1'
        self.numOfRows = 1500
        self.dust_keys = ['ServiceKey from official data portal'] # u should use ur own key
        self.station_keys = ['ServiceKey from official data portal'] # u should use ur own key
        self.key_on_use = 0



    def _dust_realtime_crawler():
        '''Gather realtime dust measurement for all stations in south korea.

        Args
        ----

        Return
        ------
        DataFrame 
        date pm10value pm10value24 pm25value pm25value24 sido station

        TODO
        ----
        '''

        # request url
        url = 'http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty?sidoName={sidoName}&pageNo={pageNo}&numOfRows={numOfRows}&ServiceKey={ServiceKey}&ver=1.3'
        locations=['서울','부산','대구','인천','광주','대전','울산','경기','강원','충북','충남','전북','전남','경북','경남','제주','세종']
        
        # variables for final dataframe
        sido_realtime_dust = {}
        sido_list = []
        stationname_list = []
        pm10value24_list = []
        pm25value24_list = []
        pm10value_list = []
        pm25value_list = []
        date_list = []
        
        # crawl for all station
        for sidoName in locations:
            Base_url = url.format(sidoName=sidoName,pageNo='1',numOfRows=self.numOfRows,
                              ServiceKey=self.dust_keys[self.key_on_use])
            # request
            r = requests.get(Base_url)
            soup_r = bs(r.text, 'lxml')
            
            for item in soup_r.find_all('item'):
                sido_list.append(sidoName)
                stationname_list.append(item.stationname.text)
                pm10value24_list.append(item.pm10value24.text)
                pm25value24_list.append(item.pm25value24.text)
                pm10value_list.append(item.pm10value.text)
                pm25value_list.append(item.pm25value.text)
                date_list.append(item.datatime.text)
        
        # input lists into the dictionary
        sido_realtime_dust['date'] = date_list
        sido_realtime_dust['sido'] = sido_list
        sido_realtime_dust['station'] = stationname_list
        sido_realtime_dust['pm10value24'] = pm10value24_list
        sido_realtime_dust['pm25value24'] = pm25value24_list
        sido_realtime_dust['pm10value'] = pm10value_list
        sido_realtime_dust['pm25value'] = pm25value_list
        
        return pd.DataFrame(sido_realtime_dust)

    def _dust_history_crawler(df):
        '''Crawls past 3 month dust measure of all stations.
     
        Args
        ----
        DataFrame with colname 'sido','station'
        
        Return
        ------
        DataFrame
        date(str) sido(str) station(str) pm10(float) pm25(float)
        
        TODO
        ----
        change date to datetime object. For this, change 6/16 24:00->6/17 00:00
        '''
        # request url
        url = 'http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty?stationName={stationName}&dataTerm={term}&pageNo={pageNo}&numOfRows={numOfRows}&ServiceKey={ServiceKey}&ver={ver}'

        # variables for final dataframe
        dust_history = {}
        date_list = []
        sido_list = []
        station_list = []
        pm25_list = []
        pm10_list = []
        
        # Build the list
        for station_index in df.index:
            location = df.loc[station_index,:]
            # format url
            Base_url = url.format(stationName=location['station'],term=self.term,pageNo=self.pageNo,
                                  numOfRows=self.numOfRows, ver='1.3', ServiceKey=self.dust_keys[self.key_on_use])
            # request
            r = requests.get(Base_url)
            soup_r = bs(r.text, 'lxml')
            
            # append date, pm25, pm10
            for item in soup_r.find_all('item'):
                ##date = datetime.datetime.strptime(item.datatime.text, '%Y-%m-%d %H:%M')
                pm25 = item.pm25value.text
                pm10 = item.pm10value.text

                # change string to float
                if pm25 != '-':
                    pm25 = float(pm25)
                else:
                    pm25 = float('nan')
                if pm10 != '-':
                    pm10 = float(pm10)
                else:
                    pm10 = float('nan')
                date_list.append(item.datatime.text)
                pm25_list.append(pm25)
                pm10_list.append(pm10)
                sido_list.append(location['sido'])
                station_list.append(location['station'])
        
        # Put all list in the dictionary
        dust_history['date'] = date_list
        dust_history['sido'] = sido_list
        dust_history['station'] = station_list
        dust_history['pm25value'] = pm25_list
        dust_history['pm10value'] = pm10_list
        
        return pd.DataFrame(dust_history)

    def station_info_crawler(df):
        '''Crawls meta information of all stations.
     
        Args
        ----
        DataFrame with colname 'sido','station'
        
        Return
        ------
        DataFrame
        sido(str) station(str) station_type(str) item_list(str) lng(str) lat(str)
        
        TODO
        ----
        '''
        
        # request url
        url = 'http://openapi.airkorea.or.kr/openapi/services/rest/MsrstnInfoInqireSvc/getMsrstnList?addr={sido}&stationName={stationName}&pageNo={pageNo}&numOfRows={n_rows}&ServiceKey={ServiceKey}'

        # variables for final dataframe
        station_info = {}
        sido_list = []
        stationname_list = []
        address_list = []
        type_list = []
        item_list = []
        lng_list = []
        lat_list = []

        # crawl for all station
        for station_index in df.index:
            try:
                location = df.loc[station_index,:]
                Base_url = url.format(sido=location['sido'],stationName=location['station'],pageNo=self.pageNo,
                                      n_rows=self.numOfRows,ServiceKey=self.station_keys[self.key_on_use])
                # request
                r = requests.get(Base_url)
                soup_r = bs(r.text, 'lxml')
                
                # get values
                address = soup_r.addr.text
                types = soup_r.mangname.text
                items = soup_r.item.item.text
                longitude = soup_r.dmy.text # 원래 x=lng이지만, 얘네가 이딴 식으로 줌.
                latitude = soup_r.dmx.text
                
                # append to the list    
                sido_list.append(location['sido'])
                stationname_list.append(location['station'])
                address_list.append(address)
                type_list.append(types)
                item_list.append(items)
                lng_list.append(longitude)
                lat_list.append(latitude)
            except:
                pass
        
        station_info['sido'] = sido_list
        station_info['station'] = stationname_list
        station_info['station_type'] = type_list
        station_info['item_list'] = item_list
        station_info['lng'] = lng_list
        station_info['lat'] = lat_list

        return pd.DataFrame(station_info)
