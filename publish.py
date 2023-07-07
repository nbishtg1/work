import numpy as np
import pandas as pd
import time
from breeze_connect import BreezeConnect
import time
from redis import Redis
import json
import logging
import datetime
import os
import multiprocessing as mp
from pymongo import MongoClient
from configparser import ConfigParser

redisconn= Redis(host="localhost", port= 6379,decode_responses=True)
# redisconn.set("NIFTY06JUL2319100CE", 85)
# z=float(redisconn.get("NIFTY 50"))
# print(type(z))
time.sleep(5)
# m=2
# # for i in range(3):
# #    time.sleep(3)
z={"symbol":"NIFTY06JUL2319100CE","action":"SELL","quantity":50,"limitPrice":85,"algoName":"HopperN"}
redisconn.publish('algo_order',json.dumps(z))



# logFileName = f'./logdata/'
# try:
#     if not os.path.exists(logFileName):
#         os.makedirs(logFileName)
# except Exception as e:
#     print(e)  

# humanTime= datetime.datetime.now()
# todaydate=str(humanTime.date())
# todaydate=todaydate.replace(':', '')
# logFileName+=f'{todaydate}_logfile.log'
            
# logging.basicConfig(level=logging.DEBUG, filename=logFileName,
#         format="[%(levelname)s]: %(message)s")
   
# configReader = ConfigParser()
# configReader.read('/home/ubuntu/work/config.ini')

# token = configReader.get('userDetail', r'token')
# # api_key = configReader.get('userDetail', r'api_key')
# # api_secret= configReader.get('userDetail', r'api_secret')

# breeze = BreezeConnect(api_key='77227i755v8b97V^9V22t705n9@Z6968')
# breeze.generate_session(api_secret='33R66117N2P1EG19OV9v21109t3&87)B',session_token=token)
# breeze.ws_connect()

# z=breeze.get_portfolio_positions()

# logging.info(z)
# OHLCMain = pd.DataFrame(z['Success'])
# logging.info(OHLCMain.to_string())