
import pandas as pd
from pymongo import MongoClient
from dataLogger import logData
from pymongo import MongoClient
from configparser import ConfigParser

def getSym(symSide,baseSym,priceReq,configReader):
    logData(f'Price find inputs {symSide,baseSym,priceReq}')
    try:
        # configReader = ConfigParser()
        # configReader.read('config.ini')
        conn = MongoClient(host = configReader['DBParams']['mongoHost'],  port = int(configReader['DBParams']['mongoPort']), username = configReader['DBParams']['mongoName'], password = configReader['DBParams']['mongoPass'])
        fetchDataDb = conn[f'InstrumentData']
        pricesData = fetchDataDb['LastTradedPrices'].find({'$and':[{'baseSym':baseSym}, {'symSide':symSide},{'LastTradedPrice':{'$lte':1.1*priceReq}},
                            {'LastTradedPrice':{'$ne':0}}]})
        df = pd.DataFrame(list(pricesData))
        df.sort_values(by = ['LastTradedPrice'],ascending=False,inplace = True)
        logData(f'Found df {df.to_string()}')
        if df['LastTradedPrice'].iloc[0]<=(priceReq*1.1):
            return df['symbolName'].iloc[0]
        else:
            return 'NotFound'

    except Exception as e:
        logData(e,'exception')
        return 'NotFound'

def getSymbyPrice(symSide,baseSym,priceReq,lesser_grater,configReader):
    logData(f'Price find inputs {symSide,baseSym,priceReq}')
    try:
        # configReader = ConfigParser()
        # configReader.read('config.ini')
        conn = MongoClient(host = configReader['DBParams']['mongoHost'],  port = int(configReader['DBParams']['mongoPort']), username = configReader['DBParams']['mongoName'], password = configReader['DBParams']['mongoPass'])
        fetchDataDb = conn[f'InstrumentData']
        if lesser_grater == "lesser":
            pricesData = fetchDataDb['LastTradedPrices'].find({'$and':[{'baseSym':baseSym}, {'symSide':symSide},{'LastTradedPrice':{'$lte':priceReq*1.05}},
                                {'LastTradedPrice':{'$ne':0}}]})
            df = pd.DataFrame(list(pricesData))
            df.sort_values(by = ['LastTradedPrice'],ascending=False,inplace = True)
            logData(f'Found df {df.to_string()}')
            if df['LastTradedPrice'].iloc[0]<=(priceReq*1.05):
                logData(df['symbolName'].iloc[0])
                return df['symbolName'].iloc[0]
            else:
                return 'NotFound'
            
        if lesser_grater== "grater":
            pricesData = fetchDataDb['LastTradedPrices'].find({'$and':[{'baseSym':baseSym}, {'symSide':symSide},{'LastTradedPrice':{'$gte':priceReq}},
                                {'LastTradedPrice':{'$ne':0}}]})
            df = pd.DataFrame(list(pricesData))
            df.sort_values(by = ['LastTradedPrice'],inplace = True)
            logData(f'Found df {df.to_string()}')
            if df['LastTradedPrice'].iloc[0]>=(priceReq):
                logData(df['symbolName'].iloc[0])
                return df['symbolName'].iloc[0]
            else:
                return 'NotFound'
    except Exception as e:
        logData(e,'exception')
        return 'NotFound'

if __name__ == '__main__':
    
    print(getSym('PE','BANKNIFTY19MAY22',100))