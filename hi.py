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


todaydate=datetime.date.today()

client = MongoClient("mongodb://localhost:27017")
print(client)
collection= client["order"][f"order_{todaydate}"]

# collection=client["OHLC_minute_1"]["NIFTY 50"]
# collection.insert_one( {'ti': 1683615480, 'low': 43452.7, 'high': 43473.65, 'open': 43470.6, 'close': 43452.7})
OHLCMain = pd.DataFrame(collection.find())
# collection = client["work"]["price"]
# post={"nifty":17000,"banknifty":38000}
# collection.insert_one(post)
# print(collection.count_documents({}))
# print(time.time())
# z=db.find({"filled":False})
# print(z)
# print(time.time())
# OHLCMain = pd.DataFrame(db.find({"filled":True}))
print(OHLCMain.to_string())

