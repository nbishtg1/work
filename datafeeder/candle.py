import numpy as np
import pandas as pd
import time
from breeze_connect import BreezeConnect
import time
import redis
import json
from pymongo import MongoClient
import datetime 
import logging
import os
from configparser import ConfigParser
from redis import Redis

logFileName = f'./logdata/'
try:
    if not os.path.exists(logFileName):
        os.makedirs(logFileName)
except Exception as e:
    print(e)  

humanTime= datetime.datetime.now()
st=str(humanTime.date())
st=st.replace(':', '')
logFileName+=f'{st}_logfile.log'
            
logging.basicConfig(level=logging.DEBUG, filename=logFileName,
        format="[%(levelname)s]: %(message)s")


configReader = ConfigParser()
configReader.read('/root/work/config.ini')

token = configReader.get('userDetail', r'token')
# api_key = configReader.get('userDetail', r'api_key')
# api_secret= configReader.get('userDetail', r'api_secret')

breeze = BreezeConnect(api_key='77227i755v8b97V^9V22t705n9@Z6968')
breeze.generate_session(api_secret='33R66117N2P1EG19OV9v21109t3&87)B',session_token=token)
breeze.ws_connect()


redisconn= Redis(host="localhost", port= 6379,decode_responses=False)
client = MongoClient("mongodb://localhost:27017")

candleDB = client["OHLC_minute_1"]
    
def on_ticks(ticks):
    humanTime= datetime.datetime.now()
    if datetime.time(9,15,10) < humanTime.time()<datetime.time(15,30,10):
        if ticks["stock_code"]=="NIFTY":
            indexName="NIFTY 50"
        elif ticks["stock_code"]=="CNXBAN":
            indexName="NIFTY BANK"
        Data={'ti':int(datetime.datetime.strptime(ticks['datetime'], '%Y-%m-%d %H:%M:%S').timestamp()), 'open': float(ticks['open']), 'high': float(ticks['high']),'low': float(ticks['low']), 'close': float(ticks['close'])}   
        collection=candleDB[indexName]
        collection.insert_one(Data)
        logging.info(Data)
        key=indexName+"lastEnt"
        redisconn.set(key,Data["ti"])
        
breeze.on_ticks = on_ticks


breeze.subscribe_feeds(stock_token="4.1!NIFTY 50",interval="1minute")
breeze.subscribe_feeds(stock_token="4.1!NIFTY BANK",interval="1minute")
