import numpy as np
import pandas as pd
import time
from breeze_connect import BreezeConnect
import time
from redis import Redis
import json
from configparser import ConfigParser

configReader = ConfigParser()
configReader.read('/root/work/config.ini')

token = configReader.get('userDetail', r'token')
# api_key = configReader.get('userDetail', r'api_key')
# api_secret= configReader.get('userDetail', r'api_secret')

breeze = BreezeConnect(api_key='77227i755v8b97V^9V22t705n9@Z6968')
breeze.generate_session(api_secret='33R66117N2P1EG19OV9v21109t3&87)B',session_token=token)
breeze.ws_connect()

with open(f'idMapforDay.json', 'r') as openfile:
        Data = json.load(openfile)

redisconn= Redis(host="localhost", port= 6379,decode_responses=False)
        
def on_ticks(ticks):
    redisconn.set( Data[ticks['symbol']] , ticks['last'],ex=100)
    
    if Data[ticks['symbol']]=="NIFTY 50":
        print(f"{Data[ticks['symbol']]} :{ticks['last']}")
   
# Assign the callbacks.
breeze.on_ticks = on_ticks

for i in Data.keys():
    breeze.subscribe_feeds(stock_token=i)
    

