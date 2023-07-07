import csv
import datetime
from datetime import date
import json
import logging
import multiprocessing as mp
import os
import time
from configparser import ConfigParser
from dataLogger import logData
from math import ceil, floor, sqrt
import pandas as pd
from redis import Redis
from pymongo import MongoClient
from expirytools import getCurrentExpiry

redisconn = Redis(host="localhost", port= 6379, decode_responses=True)



        
    
    
def getSym(symWithExpiry, side, reqPrice):
    lt = redisconn.keys()
    symList= list(filter(lambda x: symWithExpiry in x and x[-2:]==side , lt))
    df=pd.DataFrame(columns=["symbol","price"])
    for sym in symList:
        if float(redisconn.get(sym))<(reqPrice*1.1):
            df.loc[len(df)]= [sym] + [float(redisconn.get(sym))]
    if not df.empty:
        df.sort_values(by = ['price'], ascending=False, inplace = True) 
        df.reset_index(inplace=True,drop=True)  
        logging.info(df.to_string())
        print(df.to_string())
        return df['symbol'].iloc[0]
    else:
        return "not Found"
print(time.time())
print(getSym(symWithExpiry="NIFTY28JUN23", side="PE", reqPrice=100)) 
print(time.time())