import datetime
from pymongo import MongoClient
import pandas as pd

'''Timestamp inputs input should be given as seconds 
    since UNIX epoch wherever required'''

def getHistData1Min(timestamp, symbol,conn = None):
    '''Used to fetch data for a single symbol at a 
        particular time, returns data as a dictionary'''
    
    if not conn:
        conn = MongoClient()
        '''Provide the connection object else the code 
            will try to connect to MongoDB running on localhost'''
    
    db = conn['OHLC_MINUTE_1_New']
    collection = db.Data
    rec = collection.find_one(
            {'$and':[{'sym':symbol},
            {"ti": timestamp}]})
    
    if rec:
        return rec
    else:
        for i in range(10):
            rec = collection.find_one(
                    {'$and':[{'sym':symbol},
                    {"ti": timestamp - (i*60)}]})
            if rec:
                return rec
        raise Exception (f"Data not found for {symbol} at {datetime.datetime.fromtimestamp(timestamp)}")


def getHistData5Min(timestamp, symbol,conn = None):
    '''Used to fetch data for a single symbol at a 
        particular time, returns data as a dictionary'''
    
    if not conn:
        conn = MongoClient()
        '''Provide the connection object else the code 
            will try to connect to MongoDB running on localhost'''
    
    db = conn['OHLC_MINUTE_5_New']
    collection = db.Data
    rec = collection.find_one(
            {'$and':[{'sym':symbol},
            {"ti": timestamp}]})
    
    if rec:
        return rec
    else:
        raise Exception (f"Data not found for {symbol} at {datetime.datetime.fromtimestamp(timestamp)}")    

def getHistData1Day(timestamp, symbol,conn = None):
    '''Used to fetch data for a single symbol at a 
        particular time, returns data as a dictionary'''
    
    if not conn:
        conn = MongoClient()
        '''Provide the connection object else the code 
            will try to connect to MongoDB running on localhost'''
    
    db = conn['OHLC_DAY_1']
    collection = db.Data
    rec = collection.find_one(
            {'$and':[{'sym':symbol},
            {"ti": timestamp}]})
    
    if rec:
        return rec
    else:
        raise Exception (f"Data not found for {symbol} at {datetime.datetime.fromtimestamp(timestamp)}") 
    
def getBackTestData1Min(startDateTime, endDateTime, symbol, conn = None ):
    '''Used to fetch data for a single symbol 
        for a given date range, returns data as 
        a pandas dataframe'''
    try:
        if not conn:
            conn = MongoClient()
            '''Provide the connection object else the code 
                will try to connect to MongoDB running on localhost'''
        
        db = conn['OHLC_MINUTE_1_New']
        collection = db.Data
        
        rec = collection.find(
                {'$and':
                [
                    {'sym':symbol},
                    {"ti":{'$gte':startDateTime,'$lte':endDateTime}}
                ]
                })
        
        if rec:
            df = pd.DataFrame(list(rec))
            return df
        else:
            raise Exception (f"Data not found for {symbol} in range {datetime.datetime.fromtimestamp(startDateTime)} to {datetime.datetime.fromtimestamp(endDateTime)}")
    except Exception as e:
        print(e)    

def getBackTestData5Min(startDateTime, endDateTime, symbol, conn = None ):
    '''Used to fetch data for a single symbol 
        for a given date range, returns data as 
        a pandas dataframe'''
    
    if not conn:
        conn = MongoClient()
        '''Provide the connection object else the code 
            will try to connect to MongoDB running on localhost'''
    
    db = conn['OHLC_MINUTE_5_New']
    collection = db.Data
    
    rec = collection.find(
            {'$and':
            [
                {'sym':symbol},
                {"ti":{'$gte':startDateTime,'$lte':endDateTime}}
            ]
            })
    
    if rec:
        df = pd.DataFrame(list(rec))
        return df
    else:
        raise Exception (f"Data not found for {symbol} in range {datetime.datetime.fromtimestamp(startDateTime)} to {datetime.datetime.fromtimestamp(endDateTime)}")


def getBackTestData15Min(startDateTime, endDateTime, symbol, conn = None ):
    '''Used to fetch data for a single symbol 
        for a given date range, returns data as 
        a pandas dataframe'''
    
    if not conn:
        conn = MongoClient()
        '''Provide the connection object else the code 
            will try to connect to MongoDB running on localhost'''
    
    db = conn['OHLC_MINUTE_15_IND']
    collection = db.Data
    
    rec = collection.find(
            {'$and':
            [
                {'sym':symbol},
                {"ti":{'$gte':startDateTime,'$lte':endDateTime}}
            ]
            })
    
    if rec:
        df = pd.DataFrame(list(rec))
        return df
    else:
        raise Exception (f"Data not found for {symbol} in range {datetime.datetime.fromtimestamp(startDateTime)} to {datetime.datetime.fromtimestamp(endDateTime)}")
    

def getBackTestData30Min(startDateTime, endDateTime, symbol, conn = None ):
    '''Used to fetch data for a single symbol 
        for a given date range, returns data as 
        a pandas dataframe'''
    
    if not conn:
        conn = MongoClient()
        '''Provide the connection object else the code 
            will try to connect to MongoDB running on localhost'''
    
    db = conn['OHLC_MINUTE_30_IND']
    collection = db.Data
    
    rec = collection.find(
            {'$and':
            [
                {'sym':symbol},
                {"ti":{'$gte':startDateTime,'$lte':endDateTime}}
            ]
            })
    
    if rec:
        df = pd.DataFrame(list(rec))
        return df
    else:
        raise Exception (f"Data not found for {symbol} in range {datetime.datetime.fromtimestamp(startDateTime)} to {datetime.datetime.fromtimestamp(endDateTime)}")


def getBackTestData60Min(startDateTime, endDateTime, symbol, conn = None ):
    '''Used to fetch data for a single symbol 
        for a given date range, returns data as 
        a pandas dataframe'''
    
    if not conn:
        conn = MongoClient()
        '''Provide the connection object else the code 
            will try to connect to MongoDB running on localhost'''
    
    db = conn['OHLC_MINUTE_60_New']
    collection = db.Data
    
    rec = collection.find(
            {'$and':
            [
                {'sym':symbol},
                {"ti":{'$gte':startDateTime,'$lte':endDateTime}}
            ]
            })
    
    if rec:
        df = pd.DataFrame(list(rec))
        return df
    else:
        raise Exception (f"Data not found for {symbol} in range {datetime.datetime.fromtimestamp(startDateTime)} to {datetime.datetime.fromtimestamp(endDateTime)}")
    

def getBackTestData1Day(startDateTime, endDateTime, symbol, conn = None ):
    '''Used to fetch data for a single symbol 
        for a given date range, returns data as 
        a pandas dataframe'''
    
    if not conn:
        conn = MongoClient()
        '''Provide the connection object else the code 
            will try to connect to MongoDB running on localhost'''
    
    db = conn['OHLC_DAY_1']
    collection = db.Data
    
    rec = collection.find(
            {'$and':
            [
                {'sym':symbol},
                {"ti":{'$gte':startDateTime,'$lte':endDateTime}}
            ]
            })
    
    if rec:
        df = pd.DataFrame(list(rec))
        return df
    else:
        raise Exception (f"Data not found for {symbol} in range {datetime.datetime.fromtimestamp(startDateTime)} to {datetime.datetime.fromtimestamp(endDateTime)}")
    
    
def getCurrentExpiry(timestamp):
    
    ''' Used to get the option expiry for a particular date'''
    
    testDate = datetime.datetime.fromtimestamp(timestamp)
    
    date = int(testDate.strftime('%d'))
    month = int(testDate.strftime('%m'))
    year = int(testDate.strftime('%Y'))
    if datetime.date(year, month, date) <= datetime.date(2019,4,4):
        return '04APR19'
    if datetime.date(year, month, date) <= datetime.date(2019,4,11):
        return '11APR19'
    if datetime.date(year, month, date) <= datetime.date(2019,4,18):
        return '18APR19'
    if datetime.date(year, month, date) <= datetime.date(2019,4,25):
        return '25APR19'
    
    if datetime.date(year, month, date) <= datetime.date(2019,5,2):
        return '02MAY19'
    
    if datetime.date(year, month, date) <= datetime.date(2019,5,9):
        return '09MAY19'
    if datetime.date(year, month, date) <= datetime.date(2019,5,16):
        return '16MAY19'

    if datetime.date(year, month, date) <= datetime.date(2019,5,23):
        return '23MAY19'
    
    if datetime.date(year, month, date) <= datetime.date(2019,5,30):
        return '30MAY19'
    if datetime.date(year, month, date) <= datetime.date(2019,6,6):
        return '06JUN19' 
    if datetime.date(year, month, date) <= datetime.date(2019,6,13):
        return '13JUN19' 
    if datetime.date(year, month, date) <= datetime.date(2019,6,20):
        return '20JUN19' 
    if datetime.date(year, month, date) <= datetime.date(2019,6,27):
        return '27JUN19' 
    if datetime.date(year, month, date) <= datetime.date(2019,7,4):
        return '04JUL19'
    if datetime.date(year, month, date) <= datetime.date(2019,7,11):
        return '11JUL19' 
    
    if datetime.date(year, month, date) <= datetime.date(2019,7,18):
        return '18JUL19' 
    if datetime.date(year, month, date) <= datetime.date(2019,7,25):
        return '25JUL19' 
    if datetime.date(year, month, date) <= datetime.date(2019,8,1):
        return '01AUG19' 
    if datetime.date(year, month, date) <= datetime.date(2019,8,8):
        return '08AUG19' 
    if datetime.date(year, month, date) <= datetime.date(2019,8,14):
        return '14AUG19' 
    if datetime.date(year, month, date) <= datetime.date(2019,8,22):
        return '22AUG19' 
    if datetime.date(year, month, date) <= datetime.date(2019,8,29):
        return '29AUG19' 
    if datetime.date(year, month, date) <= datetime.date(2019,9,5):
        return '05SEP19' 
    if datetime.date(year, month, date) <= datetime.date(2019,9,12):
        return '12SEP19' 
    if datetime.date(year, month, date) <= datetime.date(2019,9,19):
        return '19SEP19' 
    if datetime.date(year, month, date) <= datetime.date(2019,9,26):
        return '26SEP19' 
    if datetime.date(year, month, date) <= datetime.date(2019,10,3):
        return '03OCT19'
    if datetime.date(year, month, date) <= datetime.date(2019,10,10):
        return '10OCT19'
    if datetime.date(year, month, date) <= datetime.date(2019,10,17):
        return '17OCT19' 
    if datetime.date(year, month, date) <= datetime.date(2019,10,24):
        return '24OCT19'
    if datetime.date(year, month, date) <= datetime.date(2019,10,31):
        return '31OCT19'
    if datetime.date(year, month, date) <= datetime.date(2019,11,7):
        return '07NOV19'
    if datetime.date(year, month, date) <= datetime.date(2019,11,14):
        return '14NOV19'
    if datetime.date(year, month, date) <= datetime.date(2019,11,21):
        return '21NOV19'
    if datetime.date(year, month, date) <= datetime.date(2019,11,28):
        return '28NOV19'
    if datetime.date(year, month, date) <= datetime.date(2019,12,5):
        return '05DEC19'
    if datetime.date(year, month, date) <= datetime.date(2019,12,12):
        return '12DEC19'
    if datetime.date(year, month, date) <= datetime.date(2019,12,19):
        return '19DEC19'
    if datetime.date(year, month, date) <= datetime.date(2019,12,26):
        return '26DEC19'
    if datetime.date(year, month, date) <= datetime.date(2020,1,2):
        return '02JAN20'
    if datetime.date(year, month, date) <= datetime.date(2020,1,9):
        return '09JAN20'
    if datetime.date(year, month, date) <= datetime.date(2020,1,16):
        return '16JAN20'
    if datetime.date(year, month, date) <= datetime.date(2020,1,23):
        return '23JAN20'
    if datetime.date(year, month, date) <= datetime.date(2020,1,30):
        return '30JAN20'
    if datetime.date(year, month, date) <= datetime.date(2020,2,6):
        return '06FEB20'
    if datetime.date(year, month, date) <= datetime.date(2020,2,13):
        return '13FEB20'
    if datetime.date(year, month, date) <= datetime.date(2020,2,20):
        return '20FEB20'
    if datetime.date(year, month, date) <= datetime.date(2020,2,27):
        return '27FEB20'   
    if datetime.date(year, month, date) <= datetime.date(2020, 3, 5):
        return '05MAR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 3, 12):
        return '12MAR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 3, 19):
        return '19MAR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 3, 26):
        return '26MAR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 4, 1):
        return '01APR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 4, 9):
        return '09APR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 4, 16):
        return '16APR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 4, 23):
        return '23APR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 4, 30):
        return '30APR20'
    if datetime.date(year, month, date) <= datetime.date(2020, 5, 7):
        return '07MAY20'
    if datetime.date(year, month, date) <= datetime.date(2020, 5, 14):
        return '14MAY20'
    if datetime.date(year, month, date) <= datetime.date(2020, 5, 21):
        return '21MAY20'
    if datetime.date(year, month, date) <= datetime.date(2020, 5, 28):
        return '28MAY20'
    if datetime.date(year, month, date) <= datetime.date(2020, 6, 4):
        return '04JUN20'
    if datetime.date(year, month, date) <= datetime.date(2020, 6,11):
        return '11JUN20'
    if datetime.date(year, month, date) <= datetime.date(2020, 6,18):
        return '18JUN20'
    if datetime.date(year, month, date) <= datetime.date(2020, 6,25):
        return '25JUN20'
    if datetime.date(year, month, date) <= datetime.date(2020, 7,2):
        return '02JUL20'
    if datetime.date(year, month, date) <= datetime.date(2020, 7,9):
        return '09JUL20'
    if datetime.date(year, month, date) <= datetime.date(2020, 7,16):
        return '16JUL20'
    if datetime.date(year, month, date) <= datetime.date(2020, 7,23):
        return '23JUL20'
    if datetime.date(year, month, date) <= datetime.date(2020, 7,30):
        return '30JUL20'
    if datetime.date(year, month, date) <= datetime.date(2020, 8,6):
        return '06AUG20'
    if datetime.date(year, month, date) <= datetime.date(2020,8,13):
        return '13AUG20'
    if datetime.date(year, month, date) <= datetime.date(2020,8,20):
        return '20AUG20'
    if datetime.date(year, month, date) <= datetime.date(2020,8,27):
        return '27AUG20'
    if datetime.date(year, month, date) <= datetime.date(2020,9,3):
        return '03SEP20'
    if datetime.date(year, month, date) <= datetime.date(2020,9,10):
        return '10SEP20'
    if datetime.date(year, month, date) <= datetime.date(2020,9,17):
        return '17SEP20'
    if datetime.date(year, month, date) <= datetime.date(2020,9,24):
        return '24SEP20'
    if datetime.date(year, month, date) <= datetime.date(2020,10,1):
        return '01OCT20'
    if datetime.date(year, month, date) <= datetime.date(2020,10,8):
        return '08OCT20'
    if datetime.date(year, month, date) <= datetime.date(2020,10,15):
        return '15OCT20'
    if datetime.date(year, month, date) <= datetime.date(2020,10,22):
        return '22OCT20'
    if datetime.date(year, month, date) <= datetime.date(2020,10,29):
        return '29OCT20'
    if datetime.date(year, month, date) <= datetime.date(2020,11,5):
        return '05NOV20'
    if datetime.date(year, month, date) <= datetime.date(2020,11,12):
        return '12NOV20'
    if datetime.date(year, month, date) <= datetime.date(2020,11,19):
        return '19NOV20'
    if datetime.date(year, month, date) <= datetime.date(2020,11,26):
        return '26NOV20'
    if datetime.date(year, month, date) <= datetime.date(2020,12,3):
        return '03DEC20'
    if datetime.date(year, month, date) <= datetime.date(2020,12,10):
        return '10DEC20'
    if datetime.date(year, month, date) <= datetime.date(2020,12,17):
        return '17DEC20'
    if datetime.date(year, month, date) <= datetime.date(2020,12,24):
        return '24DEC20'
    if datetime.date(year, month, date) <= datetime.date(2020,12,31):
        return '31DEC20'
    if datetime.date(year, month, date) <= datetime.date(2021,1,7):
        return '07JAN21'
    if datetime.date(year, month, date) <= datetime.date(2021,1,14):
        return '14JAN21'
    if datetime.date(year, month, date) <= datetime.date(2021,1,21):
        return '21JAN21'
    if datetime.date(year, month, date) <= datetime.date(2021,1,28):
        return '28JAN21'
    if datetime.date(year, month, date) <= datetime.date(2021,2,4):
        return '04FEB21'
    if datetime.date(year, month, date) <= datetime.date(2021,2,11):
        return '11FEB21'
    if datetime.date(year, month, date) <= datetime.date(2021,2,18):
        return '18FEB21'
    if datetime.date(year, month, date) <= datetime.date(2021,2,25):
        return '25FEB21'
    if datetime.date(year, month, date) <= datetime.date(2021,3,4):
        return '04MAR21'
    if datetime.date(year, month, date) <= datetime.date(2021,3,10):
        return '10MAR21'
    if datetime.date(year, month, date) <= datetime.date(2021,3,18):
        return '18MAR21'
    if datetime.date(year, month, date) <= datetime.date(2021,3,25):
        return '25MAR21'
    if datetime.date(year, month, date) <= datetime.date(2021,4,1):
        return '01APR21'
    if datetime.date(year, month, date) <= datetime.date(2021,4,8):
        return '08APR21'
    if datetime.date(year, month, date) <= datetime.date(2021,4,15):
        return '15APR21'
    if datetime.date(year, month, date) <= datetime.date(2021,4,22):
        return '22APR21'
    if datetime.date(year, month, date) <= datetime.date(2021,4,29):
        return '29APR21'
    if datetime.date(year, month, date) <= datetime.date(2021,5,6):
        return '06MAY21'
    if datetime.date(year, month, date) <= datetime.date(2021,5,12):
        return '12MAY21'
    if datetime.date(year, month, date) <= datetime.date(2021,5,20):
        return '20MAY21'
    if datetime.date(year, month, date) <= datetime.date(2021,5,27):
        return '27MAY21'
    if datetime.date(year, month, date) <= datetime.date(2021,6,3):
        return '03JUN21'
    if datetime.date(year, month, date) <= datetime.date(2021,6,10):
        return '10JUN21'
    if datetime.date(year, month, date) <= datetime.date(2021,6,17):
        return '17JUN21'
    if datetime.date(year, month, date) <= datetime.date(2021,6,24):
        return '24JUN21'
    if datetime.date(year, month, date) <= datetime.date(2021,7,1):
        return '01JUL21'
    if datetime.date(year, month, date) <= datetime.date(2021,7,8):
        return '08JUL21'
    if datetime.date(year, month, date) <= datetime.date(2021,7,15):
        return '15JUL21'
    if datetime.date(year, month, date) <= datetime.date(2021,7,22):
        return '22JUL21'
    if datetime.date(year, month, date) <= datetime.date(2021,7,29):
        return '29JUL21'
        
    if datetime.date(year, month, date) <= datetime.date(2021,8,5):
        return '05AUG21'
    if datetime.date(year, month, date) <= datetime.date(2021,8,12):
        return '12AUG21'
    if datetime.date(year, month, date) <= datetime.date(2021,8,18):
        return '18AUG21'
    if datetime.date(year, month, date) <= datetime.date(2021,8,26):
        return '26AUG21'

    if datetime.date(year, month, date) <= datetime.date(2021,9,2):
        return '02SEP21'
    
    if datetime.date(year, month, date) <= datetime.date(2021,9,9):
        return '09SEP21'
    
    if datetime.date(year, month, date) <= datetime.date(2021,9,16):
        return '16SEP21'

    if datetime.date(year, month, date) <= datetime.date(2021,9,23):
        return '23SEP21'

    if datetime.date(year, month, date) <= datetime.date(2021,9,30):
        return '30SEP21'
    if datetime.date(year, month, date) <= datetime.date(2021,10,7):
        return '07OCT21'
    if datetime.date(year, month, date) <= datetime.date(2021,10,14):
        return '14OCT21'
    if datetime.date(year, month, date) <= datetime.date(2021,10,21):
        return '21OCT21'
    if datetime.date(year, month, date) <= datetime.date(2021,10,28):
        return '28OCT21'
    if datetime.date(year, month, date) <= datetime.date(2021,11,3):
        return '03NOV21'
    if datetime.date(year, month, date) <= datetime.date(2021,11,11):
        return '11NOV21'
    if datetime.date(year, month, date) <= datetime.date(2021,11,18):
        return '18NOV21'
    if datetime.date(year, month, date) <= datetime.date(2021,11,25):
        return '25NOV21'
    if datetime.date(year, month, date) <= datetime.date(2021,12,2):
        return '02DEC21'
    if datetime.date(year, month, date) <= datetime.date(2021,12,9):
        return '09DEC21'
    if datetime.date(year, month, date) <= datetime.date(2021,12,16):
        return '16DEC21'
    if datetime.date(year, month, date) <= datetime.date(2021,12,23):
        return '23DEC21'
    if datetime.date(year, month, date) <= datetime.date(2021,12,30):
        return '30DEC21'
    if datetime.date(year, month, date) <= datetime.date(2022,1,6):
        return '06JAN22'
    if datetime.date(year, month, date) <= datetime.date(2022,1,13):
        return '13JAN22'
    if datetime.date(year, month, date) <= datetime.date(2022,1,20):
        return '20JAN22'
    if datetime.date(year, month, date) <= datetime.date(2022,1,27):
        return '27JAN22'
    if datetime.date(year, month, date) <= datetime.date(2022,2,3):
        return '03FEB22'
    if datetime.date(year, month, date) <= datetime.date(2022,2,10):
        return '10FEB22'
    if datetime.date(year, month, date) <= datetime.date(2022,2,17):
        return '17FEB22'
    if datetime.date(year, month, date) <= datetime.date(2022,2,24):
        return '24FEB22'
    if datetime.date(year, month, date) <= datetime.date(2022,3,3):
        return '03MAR22'
    if datetime.date(year, month, date) <= datetime.date(2022,3,10):
        return '10MAR22'
    if datetime.date(year, month, date) <= datetime.date(2022,3,17):
        return '17MAR22'
    if datetime.date(year, month, date) <= datetime.date(2022,3,24):
        return '24MAR22'
    if datetime.date(year, month, date) <= datetime.date(2022,3,31):
        return '31MAR22'
    if datetime.date(year, month, date) <= datetime.date(2022,4,7):
        return '07APR22'
    if datetime.date(year, month, date) <= datetime.date(2022,4,13):
        return '13APR22'
    if datetime.date(year, month, date) <= datetime.date(2022,4,21):
        return '21APR22'
    if datetime.date(year, month, date) <= datetime.date(2022,4,28):
        return '28APR22'
    if datetime.date(year, month, date) <= datetime.date(2022,5,5):
        return '05MAY22'
    if datetime.date(year, month, date) <= datetime.date(2022,5,12):
        return '12MAY22'
    if datetime.date(year, month, date) <= datetime.date(2022,5,19):
        return '19MAY22'
    if datetime.date(year, month, date) <= datetime.date(2022,5,26):
        return '26MAY22'
    if datetime.date(year, month, date) <= datetime.date(2022,6,2):
        return '02JUN22'
    if datetime.date(year, month, date) <= datetime.date(2022,6,9):
        return '09JUN22'
    if datetime.date(year, month, date) <= datetime.date(2022,6,16):
        return '16JUN22'
    if datetime.date(year, month, date) <= datetime.date(2022,6,23):
        return '23JUN22'
    if datetime.date(year, month, date) <= datetime.date(2022,6,30):
        return '30JUN22'    
    if datetime.date(year, month, date) <= datetime.date(2022,7,7):
        return '07JUL22'
    if datetime.date(year, month, date) <= datetime.date(2022,7,14):
        return '14JUL22'

    if datetime.date(year, month, date) <= datetime.date(2022,7,21):
        return '21JUL22'
    if datetime.date(year, month, date) <= datetime.date(2022,7,28):
        return '28JUL22'
    if datetime.date(year, month, date) <= datetime.date(2022,8,4):
        return '04AUG22'
    if datetime.date(year, month, date) <= datetime.date(2022,8,11):
        return '11AUG22'
    if datetime.date(year, month, date) <= datetime.date(2022,8,18):
        return '18AUG22'
    if datetime.date(year, month, date) <= datetime.date(2022,8,25):
        return '25AUG22'
        
    if datetime.date(year, month, date) <= datetime.date(2022,9,1):
        return '01SEP22'
    if datetime.date(year, month, date) <= datetime.date(2022,9,8):
        return '08SEP22'
    if datetime.date(year, month, date) <= datetime.date(2022,9,15):
        return '15SEP22'
    if datetime.date(year, month, date) <= datetime.date(2022,9,22):
        return '22SEP22'
    if datetime.date(year, month, date) <= datetime.date(2022,9,29):
        return '29SEP22'
    if datetime.date(year, month, date) <= datetime.date(2022,10,6):
        return '06OCT22'
    if datetime.date(year, month, date) <= datetime.date(2022,10,13):
        return '13OCT22'
    if datetime.date(year, month, date) <= datetime.date(2022,10,20):
        return '20OCT22'
    if datetime.date(year, month, date) <= datetime.date(2022,10,27):
        return '27OCT22'      
    if datetime.date(year, month, date) <= datetime.date(2022,11,3):
        return '03NOV22'
    if datetime.date(year, month, date) <= datetime.date(2022,11,10):
        return '10NOV22'
    if datetime.date(year, month, date) <= datetime.date(2022,11,17):
        return '17NOV22'
    if datetime.date(year, month, date) <= datetime.date(2022,11,24):
        return '24NOV22'
    if datetime.date(year, month, date) <= datetime.date(2022,12,1):
        return '01DEC22'
    if datetime.date(year, month, date) <= datetime.date(2022,12,8):
        return '08DEC22'
    if datetime.date(year, month, date) <= datetime.date(2022,12,15):
        return '15DEC22'
    if datetime.date(year, month, date) <= datetime.date(2022,12,22):
        return '22DEC22'
    if datetime.date(year, month, date) <= datetime.date(2022,12,29):
        return '29DEC22'                
    raise Exception(f'Expiry not found, add data!')

def getlastdayweeklyExpiry(timestamp):
    
    ''' Used to get the option expiry for a particular date'''
    
    testDate = datetime.datetime.fromtimestamp(timestamp)
    
    date = int(testDate.strftime('%d'))
    month = int(testDate.strftime('%m'))
    year = int(testDate.strftime('%Y'))
    if datetime.date(year, month, date) == datetime.date(2019,4,4):
        return '04APR19'
    if datetime.date(year, month, date) == datetime.date(2019,4,11):
        return '11APR19'
    if datetime.date(year, month, date) == datetime.date(2019,4,18):
        return '18APR19'
    if datetime.date(year, month, date) == datetime.date(2019,4,25):
        return '25APR19'
    
    if datetime.date(year, month, date) == datetime.date(2019,5,2):
        return '02MAY19'
    
    if datetime.date(year, month, date) == datetime.date(2019,5,9):
        return '09MAY19'
    if datetime.date(year, month, date) == datetime.date(2019,5,16):
        return '16MAY19'

    if datetime.date(year, month, date) == datetime.date(2019,5,23):
        return '23MAY19'
    
    if datetime.date(year, month, date) == datetime.date(2019,5,30):
        return '30MAY19'
    if datetime.date(year, month, date) == datetime.date(2019,6,6):
        return '06JUN19' 
    if datetime.date(year, month, date) == datetime.date(2019,6,13):
        return '13JUN19' 
    if datetime.date(year, month, date) == datetime.date(2019,6,20):
        return '20JUN19' 
    if datetime.date(year, month, date) == datetime.date(2019,6,27):
        return '27JUN19' 
    if datetime.date(year, month, date) == datetime.date(2019,7,4):
        return '04JUL19'
    if datetime.date(year, month, date) == datetime.date(2019,7,11):
        return '11JUL19' 
    
    if datetime.date(year, month, date) == datetime.date(2019,7,18):
        return '18JUL19' 
    if datetime.date(year, month, date) == datetime.date(2019,7,25):
        return '25JUL19' 
    if datetime.date(year, month, date) == datetime.date(2019,8,1):
        return '01AUG19' 
    if datetime.date(year, month, date) == datetime.date(2019,8,8):
        return '08AUG19' 
    if datetime.date(year, month, date) == datetime.date(2019,8,14):
        return '14AUG19' 
    if datetime.date(year, month, date) == datetime.date(2019,8,22):
        return '22AUG19' 
    if datetime.date(year, month, date) == datetime.date(2019,8,29):
        return '29AUG19' 
    if datetime.date(year, month, date) == datetime.date(2019,9,5):
        return '05SEP19' 
    if datetime.date(year, month, date) == datetime.date(2019,9,12):
        return '12SEP19' 
    if datetime.date(year, month, date) == datetime.date(2019,9,19):
        return '19SEP19' 
    if datetime.date(year, month, date) == datetime.date(2019,9,26):
        return '26SEP19' 
    if datetime.date(year, month, date) == datetime.date(2019,10,3):
        return '03OCT19'
    if datetime.date(year, month, date) == datetime.date(2019,10,10):
        return '10OCT19'
    if datetime.date(year, month, date) == datetime.date(2019,10,17):
        return '17OCT19' 
    if datetime.date(year, month, date) == datetime.date(2019,10,24):
        return '24OCT19'
    if datetime.date(year, month, date) == datetime.date(2019,10,31):
        return '31OCT19'
    if datetime.date(year, month, date) == datetime.date(2019,11,7):
        return '07NOV19'
    if datetime.date(year, month, date) == datetime.date(2019,11,14):
        return '14NOV19'
    if datetime.date(year, month, date) == datetime.date(2019,11,21):
        return '21NOV19'
    if datetime.date(year, month, date) == datetime.date(2019,11,28):
        return '28NOV19'
    if datetime.date(year, month, date) == datetime.date(2019,12,5):
        return '05DEC19'
    if datetime.date(year, month, date) == datetime.date(2019,12,12):
        return '12DEC19'
    if datetime.date(year, month, date) == datetime.date(2019,12,19):
        return '19DEC19'
    if datetime.date(year, month, date) == datetime.date(2019,12,26):
        return '26DEC19'
    if datetime.date(year, month, date) == datetime.date(2020,1,2):
        return '02JAN20'
    if datetime.date(year, month, date) == datetime.date(2020,1,9):
        return '09JAN20'
    if datetime.date(year, month, date) == datetime.date(2020,1,16):
        return '16JAN20'
    if datetime.date(year, month, date) == datetime.date(2020,1,23):
        return '23JAN20'
    if datetime.date(year, month, date) == datetime.date(2020,1,30):
        return '30JAN20'
    if datetime.date(year, month, date) == datetime.date(2020,2,6):
        return '06FEB20'
    if datetime.date(year, month, date) == datetime.date(2020,2,13):
        return '13FEB20'
    if datetime.date(year, month, date) == datetime.date(2020,2,20):
        return '20FEB20'
    if datetime.date(year, month, date) == datetime.date(2020,2,27):
        return '27FEB20'    
    if datetime.date(year, month, date) == datetime.date(2020, 3, 5):
        return '05MAR20'
    if datetime.date(year, month, date) == datetime.date(2020, 3, 12):
        return '12MAR20'
    if datetime.date(year, month, date) == datetime.date(2020, 3, 19):
        return '19MAR20'
    if datetime.date(year, month, date) == datetime.date(2020, 3, 26):
        return '26MAR20'
    if datetime.date(year, month, date) == datetime.date(2020, 4, 1):
        return '01APR20'
    if datetime.date(year, month, date) == datetime.date(2020, 4, 9):
        return '09APR20'
    if datetime.date(year, month, date) == datetime.date(2020, 4, 16):
        return '16APR20'
    if datetime.date(year, month, date) == datetime.date(2020, 4, 23):
        return '23APR20'
    if datetime.date(year, month, date) == datetime.date(2020, 4, 30):
        return '30APR20'
    if datetime.date(year, month, date) == datetime.date(2020, 5, 7):
        return '07MAY20'
    if datetime.date(year, month, date) == datetime.date(2020, 5, 14):
        return '14MAY20'
    if datetime.date(year, month, date) == datetime.date(2020, 5, 21):
        return '21MAY20'
    if datetime.date(year, month, date) == datetime.date(2020, 5, 28):
        return '28MAY20'
    if datetime.date(year, month, date) == datetime.date(2020, 6, 4):
        return '04JUN20'
    if datetime.date(year, month, date) == datetime.date(2020, 6,11):
        return '11JUN20'
    if datetime.date(year, month, date) == datetime.date(2020, 6,18):
        return '18JUN20'
    if datetime.date(year, month, date) == datetime.date(2020, 6,25):
        return '25JUN20'
    if datetime.date(year, month, date) == datetime.date(2020, 7,2):
        return '02JUL20'
    if datetime.date(year, month, date) == datetime.date(2020, 7,9):
        return '09JUL20'
    if datetime.date(year, month, date) == datetime.date(2020, 7,16):
        return '16JUL20'
    if datetime.date(year, month, date) == datetime.date(2020, 7,23):
        return '23JUL20'
    if datetime.date(year, month, date) == datetime.date(2020, 7,30):
        return '30JUL20'
    if datetime.date(year, month, date) == datetime.date(2020, 8,6):
        return '06AUG20'
    if datetime.date(year, month, date) == datetime.date(2020,8,13):
        return '13AUG20'
    if datetime.date(year, month, date) == datetime.date(2020,8,20):
        return '20AUG20'
    if datetime.date(year, month, date) == datetime.date(2020,8,27):
        return '27AUG20'
    if datetime.date(year, month, date) == datetime.date(2020,9,3):
        return '03SEP20'
    if datetime.date(year, month, date) == datetime.date(2020,9,10):
        return '10SEP20'
    if datetime.date(year, month, date) == datetime.date(2020,9,17):
        return '17SEP20'
    if datetime.date(year, month, date) == datetime.date(2020,9,24):
        return '24SEP20'
    if datetime.date(year, month, date) == datetime.date(2020,10,1):
        return '01OCT20'
    if datetime.date(year, month, date) == datetime.date(2020,10,8):
        return '08OCT20'
    if datetime.date(year, month, date) == datetime.date(2020,10,15):
        return '15OCT20'
    if datetime.date(year, month, date) == datetime.date(2020,10,22):
        return '22OCT20'
    if datetime.date(year, month, date) == datetime.date(2020,10,29):
        return '29OCT20'
    if datetime.date(year, month, date) == datetime.date(2020,11,5):
        return '05NOV20'
    if datetime.date(year, month, date) == datetime.date(2020,11,12):
        return '12NOV20'
    if datetime.date(year, month, date) == datetime.date(2020,11,19):
        return '19NOV20'
    if datetime.date(year, month, date) == datetime.date(2020,11,26):
        return '26NOV20'
    if datetime.date(year, month, date) == datetime.date(2020,12,3):
        return '03DEC20'
    if datetime.date(year, month, date) == datetime.date(2020,12,10):
        return '10DEC20'
    if datetime.date(year, month, date) == datetime.date(2020,12,17):
        return '17DEC20'
    if datetime.date(year, month, date) == datetime.date(2020,12,24):
        return '24DEC20'
    if datetime.date(year, month, date) == datetime.date(2020,12,31):
        return '31DEC20'
    if datetime.date(year, month, date) == datetime.date(2021,1,7):
        return '07JAN21'
    if datetime.date(year, month, date) == datetime.date(2021,1,14):
        return '14JAN21'
    if datetime.date(year, month, date) == datetime.date(2021,1,21):
        return '21JAN21'
    if datetime.date(year, month, date) == datetime.date(2021,1,28):
        return '28JAN21'
    if datetime.date(year, month, date) == datetime.date(2021,2,4):
        return '04FEB21'
    if datetime.date(year, month, date) == datetime.date(2021,2,11):
        return '11FEB21'
    if datetime.date(year, month, date) == datetime.date(2021,2,18):
        return '18FEB21'
    if datetime.date(year, month, date) == datetime.date(2021,2,25):
        return '25FEB21'
    if datetime.date(year, month, date) == datetime.date(2021,3,4):
        return '04MAR21'
    if datetime.date(year, month, date) == datetime.date(2021,3,10):
        return '10MAR21'
    if datetime.date(year, month, date) == datetime.date(2021,3,18):
        return '18MAR21'
    if datetime.date(year, month, date) == datetime.date(2021,3,25):
        return '25MAR21'
    if datetime.date(year, month, date) == datetime.date(2021,4,1):
        return '01APR21'
    if datetime.date(year, month, date) == datetime.date(2021,4,8):
        return '08APR21'
    if datetime.date(year, month, date) == datetime.date(2021,4,15):
        return '15APR21'
    if datetime.date(year, month, date) == datetime.date(2021,4,22):
        return '22APR21'
    if datetime.date(year, month, date) == datetime.date(2021,4,29):
        return '29APR21'
    if datetime.date(year, month, date) == datetime.date(2021,5,6):
        return '06MAY21'
    if datetime.date(year, month, date) == datetime.date(2021,5,12):
        return '12MAY21'
    if datetime.date(year, month, date) == datetime.date(2021,5,20):
        return '20MAY21'
    if datetime.date(year, month, date) == datetime.date(2021,5,27):
        return '27MAY21'
    if datetime.date(year, month, date) == datetime.date(2021,6,3):
        return '03JUN21'
    if datetime.date(year, month, date) == datetime.date(2021,6,10):
        return '10JUN21'
    if datetime.date(year, month, date) == datetime.date(2021,6,17):
        return '17JUN21'
    if datetime.date(year, month, date) == datetime.date(2021,6,24):
        return '24JUN21'
    if datetime.date(year, month, date) == datetime.date(2021,7,1):
        return '01JUL21'
    if datetime.date(year, month, date) == datetime.date(2021,7,8):
        return '08JUL21'
    if datetime.date(year, month, date) == datetime.date(2021,7,15):
        return '15JUL21'
    if datetime.date(year, month, date) == datetime.date(2021,7,22):
        return '22JUL21'
    if datetime.date(year, month, date) == datetime.date(2021,7,29):
        return '29JUL21'
        
    if datetime.date(year, month, date) == datetime.date(2021,8,5):
        return '05AUG21'
    if datetime.date(year, month, date) == datetime.date(2021,8,12):
        return '12AUG21'
    if datetime.date(year, month, date) == datetime.date(2021,8,18):
        return '18AUG21'
    if datetime.date(year, month, date) == datetime.date(2021,8,26):
        return '26AUG21'

    if datetime.date(year, month, date) == datetime.date(2021,9,2):
        return '02SEP21'
    
    if datetime.date(year, month, date) == datetime.date(2021,9,9):
        return '09SEP21'
    
    if datetime.date(year, month, date) == datetime.date(2021,9,16):
        return '16SEP21'

    if datetime.date(year, month, date) == datetime.date(2021,9,23):
        return '23SEP21'

    if datetime.date(year, month, date) == datetime.date(2021,9,30):
        return '30SEP21'
    if datetime.date(year, month, date) == datetime.date(2021,10,7):
        return '07OCT21'
    if datetime.date(year, month, date) == datetime.date(2021,10,14):
        return '14OCT21'
    if datetime.date(year, month, date) == datetime.date(2021,10,21):
        return '21OCT21'
    if datetime.date(year, month, date) == datetime.date(2021,10,28):
        return '28OCT21'
    if datetime.date(year, month, date) == datetime.date(2021,11,3):
        return '03NOV21'
    if datetime.date(year, month, date) == datetime.date(2021,11,11):
        return '11NOV21'
    if datetime.date(year, month, date) == datetime.date(2021,11,18):
        return '18NOV21'
    if datetime.date(year, month, date) == datetime.date(2021,11,25):
        return '25NOV21'
    if datetime.date(year, month, date) == datetime.date(2021,12,2):
        return '02DEC21'
    if datetime.date(year, month, date) == datetime.date(2021,12,9):
        return '09DEC21'
    if datetime.date(year, month, date) == datetime.date(2021,12,16):
        return '16DEC21'
    if datetime.date(year, month, date) == datetime.date(2021,12,23):
        return '23DEC21'
    if datetime.date(year, month, date) == datetime.date(2021,12,30):
        return '30DEC21'
    if datetime.date(year, month, date) == datetime.date(2022,1,6):
        return '06JAN22'
    if datetime.date(year, month, date) == datetime.date(2022,1,13):
        return '13JAN22'
    if datetime.date(year, month, date) == datetime.date(2022,1,20):
        return '20JAN22'
    if datetime.date(year, month, date) == datetime.date(2022,1,27):
        return '27JAN22'
    if datetime.date(year, month, date) == datetime.date(2022,2,3):
        return '03FEB22'
    if datetime.date(year, month, date) == datetime.date(2022,2,10):
        return '10FEB22'
    if datetime.date(year, month, date) == datetime.date(2022,2,17):
        return '17FEB22'
    if datetime.date(year, month, date) == datetime.date(2022,2,24):
        return '24FEB22'
    if datetime.date(year, month, date) == datetime.date(2022,3,3):
        return '03MAR22'
    if datetime.date(year, month, date) == datetime.date(2022,3,10):
        return '10MAR22'
    if datetime.date(year, month, date) == datetime.date(2022,3,17):
        return '17MAR22'
    if datetime.date(year, month, date) == datetime.date(2022,3,24):
        return '24MAR22'
    if datetime.date(year, month, date) == datetime.date(2022,3,31):
        return '31MAR22'
    if datetime.date(year, month, date) == datetime.date(2022,4,7):
        return '07APR22'
    if datetime.date(year, month, date) == datetime.date(2022,4,13):
        return '13APR22'
    if datetime.date(year, month, date) == datetime.date(2022,4,21):
        return '21APR22'
    if datetime.date(year, month, date) == datetime.date(2022,4,28):
        return '28APR22'
    if datetime.date(year, month, date) == datetime.date(2022,5,5):
        return '05MAY22'
    if datetime.date(year, month, date) == datetime.date(2022,5,12):
        return '12MAY22'
    if datetime.date(year, month, date) == datetime.date(2022,5,19):
        return '19MAY22'
    if datetime.date(year, month, date) == datetime.date(2022,5,26):
        return '26MAY22'
    if datetime.date(year, month, date) == datetime.date(2022,6,2):
        return '02JUN22'
    if datetime.date(year, month, date) == datetime.date(2022,6,9):
        return '09JUN22'
    if datetime.date(year, month, date) == datetime.date(2022,6,16):
        return '16JUN22'
    if datetime.date(year, month, date) == datetime.date(2022,6,23):
        return '23JUN22'
    if datetime.date(year, month, date) == datetime.date(2022,6,30):
        return '30JUN22'    
    if datetime.date(year, month, date) == datetime.date(2022,7,7):
        return '07JUL22'
    if datetime.date(year, month, date) == datetime.date(2022,7,14):
        return '14JUL22'

    if datetime.date(year, month, date) == datetime.date(2022,7,21):
        return '21JUL22'
    if datetime.date(year, month, date) == datetime.date(2022,7,28):
        return '28JUL22'
    if datetime.date(year, month, date) == datetime.date(2022,8,4):
        return '04AUG22'
    if datetime.date(year, month, date) == datetime.date(2022,8,11):
        return '11AUG22'
    if datetime.date(year, month, date) == datetime.date(2022,8,18):
        return '18AUG22'
    if datetime.date(year, month, date) == datetime.date(2022,8,25):
        return '25AUG22'
        
    if datetime.date(year, month, date) == datetime.date(2022,9,1):
        return '01SEP22'
    if datetime.date(year, month, date) == datetime.date(2022,9,8):
        return '08SEP22'
    if datetime.date(year, month, date) == datetime.date(2022,9,15):
        return '15SEP22'
    if datetime.date(year, month, date) == datetime.date(2022,9,22):
        return '22SEP22'
    if datetime.date(year, month, date) == datetime.date(2022,9,29):
        return '29SEP22'
    if datetime.date(year, month, date) == datetime.date(2022,10,6):
        return '06OCT22'
    if datetime.date(year, month, date) == datetime.date(2022,10,13):
        return '13OCT22'
    if datetime.date(year, month, date) == datetime.date(2022,10,20):
        return '20OCT22'
    if datetime.date(year, month, date) == datetime.date(2022,10,27):
        return '27OCT22'      
    if datetime.date(year, month, date) == datetime.date(2022,11,3):
        return '03NOV22'
    if datetime.date(year, month, date) == datetime.date(2022,11,10):
        return '10NOV22'
    if datetime.date(year, month, date) == datetime.date(2022,11,17):
        return '17NOV22'
    if datetime.date(year, month, date) == datetime.date(2022,11,24):
        return '24NOV22'
    if datetime.date(year, month, date) == datetime.date(2022,12,1):
        return '01DEC22'
    if datetime.date(year, month, date) == datetime.date(2022,12,8):
        return '08DEC22'
    if datetime.date(year, month, date) == datetime.date(2022,12,15):
        return '15DEC22'
    if datetime.date(year, month, date) == datetime.date(2022,12,22):
        return '22DEC22'
    if datetime.date(year, month, date) == datetime.date(2022,12,29):
        return '29DEC22'                
    else:
        return "no rollover"









def getfutExpiry(timestamp):
    
    ''' Used to get the option expiry for a particular date'''
    
    testDate = datetime.datetime.fromtimestamp(timestamp)
    
    date = int(testDate.strftime('%d'))
    month = int(testDate.strftime('%m'))
    year = int(testDate.strftime('%Y'))
    
    if datetime.date(year, month, date) <= datetime.date(2020, 3, 26):
        return "20MARFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2020, 4, 30):
        return "20APRFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2020, 5, 28):
        return "20MAYFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2020, 6,25):
        return "20JUNFUT"
   
    if datetime.date(year, month, date) <= datetime.date(2020, 7,30):
        return "20JULFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2020,8,27):
        return "20AUGFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2020,9,24):
        return "20SEPFUT"

    if datetime.date(year, month, date) <= datetime.date(2020,10,29):
        return "20OCTFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2020,11,26):
        return "20NOVFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2020,12,31):
        return "20DECFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2021,1,28):
        return "21JANFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2021,2,25):
        return "21FEBFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2021,3,25):
        return "21MARFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2021,4,29):
        return "21APRFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2021,5,27):
        return "21MAYFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2021,6,24):
        return "21JUNFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2021,7,29):
        return "21JULFUT"
        
   
    if datetime.date(year, month, date) <= datetime.date(2021,8,26):
        return "21AUGFUT"

    if datetime.date(year, month, date) <= datetime.date(2021,9,30):
        return "21SEPFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2021,10,28):
        return "21OCTFUT"

    if datetime.date(year, month, date) <= datetime.date(2021,11,25):
        return "21NOVFUT"

    if datetime.date(year, month, date) <= datetime.date(2021,12,30):
        return "21DECFUT"

    if datetime.date(year, month, date) <= datetime.date(2022,1,27):
        return "22JANFUT"

    if datetime.date(year, month, date) <= datetime.date(2022,2,24):
        return "22FEBFUT"

    if datetime.date(year, month, date) <= datetime.date(2022,3,31):
        return "22MARFUT"

    if datetime.date(year, month, date) <= datetime.date(2022,4,28):
        return "22APRFUT"

    if datetime.date(year, month, date) <= datetime.date(2022,5,26):
        return "22MAYFUT"

    if datetime.date(year, month, date) <= datetime.date(2022,6,30):
        return "22JUNFUT"

    if datetime.date(year, month, date) <= datetime.date(2022,7,28):
        return "22JULFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2022,8,25):
        return "22AUGFUT"
    
    if datetime.date(year, month, date) <= datetime.date(2022,9,29):
        return "22SEPFUT"
    raise Exception(f'Expiry not found, add data!')

def getlastdayofExpiry(timestamp):
    
    ''' Used to get the option expiry for a particular date'''
    
    testDate = datetime.datetime.fromtimestamp(timestamp)
    
    date = int(testDate.strftime('%d'))
    month = int(testDate.strftime('%m'))
    year = int(testDate.strftime('%Y'))
    
    if datetime.date(year, month, date) == datetime.date(2020, 3, 26):
        return "20MARFUT"
    
    if datetime.date(year, month, date) == datetime.date(2020, 4, 30):
        return "20APRFUT"
    
    if datetime.date(year, month, date) == datetime.date(2020, 5, 28):
        return "20MAYFUT"
    
    if datetime.date(year, month, date) == datetime.date(2020, 6,25):
        return "20JUNFUT"
   
    if datetime.date(year, month, date) == datetime.date(2020, 7,30):
        return "20JULFUT"
    
    if datetime.date(year, month, date) == datetime.date(2020,8,27):
        return "20AUGFUT"
    
    if datetime.date(year, month, date) == datetime.date(2020,9,24):
        return "20SEPFUT"

    if datetime.date(year, month, date) == datetime.date(2020,10,29):
        return "20OCTFUT"
    
    if datetime.date(year, month, date) == datetime.date(2020,11,26):
        return "20NOVFUT"
    
    if datetime.date(year, month, date) == datetime.date(2020,12,31):
        return "20DECFUT"
    
    if datetime.date(year, month, date) == datetime.date(2021,1,28):
        return "21JANFUT"
    
    if datetime.date(year, month, date) == datetime.date(2021,2,25):
        return "21FEBFUT"
    
    if datetime.date(year, month, date) == datetime.date(2021,3,25):
        return "21MARFUT"
    
    if datetime.date(year, month, date) == datetime.date(2021,4,29):
        return "21APRFUT"
    
    if datetime.date(year, month, date) == datetime.date(2021,5,27):
        return "21MAYFUT"
    
    if datetime.date(year, month, date) == datetime.date(2021,6,24):
        return "21JUNFUT"
    
    if datetime.date(year, month, date) == datetime.date(2021,7,29):
        return "21JULFUT"
        
   
    if datetime.date(year, month, date) == datetime.date(2021,8,26):
        return "21AUGFUT"

    if datetime.date(year, month, date) == datetime.date(2021,9,30):
        return "21SEPFUT"
    
    if datetime.date(year, month, date) == datetime.date(2021,10,28):
        return "21OCTFUT"

    if datetime.date(year, month, date) == datetime.date(2021,11,25):
        return "21NOVFUT"

    if datetime.date(year, month, date) == datetime.date(2021,12,30):
        return "21DECFUT"

    if datetime.date(year, month, date) == datetime.date(2022,1,27):
        return "22JANFUT"

    if datetime.date(year, month, date) == datetime.date(2022,2,24):
        return "22FEBFUT"

    if datetime.date(year, month, date) == datetime.date(2022,3,31):
        return "22MARFUT"

    if datetime.date(year, month, date) == datetime.date(2022,4,28):
        return "22APRFUT"

    if datetime.date(year, month, date) == datetime.date(2022,5,26):
        return "22MAYFUT"

    if datetime.date(year, month, date) == datetime.date(2022,6,30):
        return "22JUNFUT"

    if datetime.date(year, month, date) == datetime.date(2022,7,28):
        return "22JULFUT"
    
    if datetime.date(year, month, date) == datetime.date(2022,8,25):
        return "22AUGFUT"
    
    if datetime.date(year, month, date) == datetime.date(2022,9,29):
        return "22SEPFUT"
    else:
        return "no rollover"

if __name__ == "__main__":
    
    '''Sample code for using the functions'''
    
    conn = MongoClient()

    data = getHistData1Min(datetime.datetime(2021,9,9,9,20,0).timestamp(),
                            'NIFTY 50',conn)

    print(f'Data fetched is {data}')

    dataDF = getBackTestData1Min(datetime.datetime(2021,9,9,9,20,0).timestamp(),
                                datetime.datetime(2021,9,9,15,20,0).timestamp(),
                                'NIFTY 50',conn)
   
    print(f'Data fetched is \n {dataDF.to_string()}')

    '''Get current weekly Expiry for a particular day'''

    print(f'Weekly Option expiry for the day {datetime.datetime(2021,9,9,9,20,0)} is {getCurrentExpiry(datetime.datetime(2021,9,9,9,20,0).timestamp())}')
