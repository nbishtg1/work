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
from addposition import updatePosition


todaydate = datetime.date.today()

client = MongoClient("mongodb://localhost:27017")
orderDB = client["order"][f"order_{todaydate}"]
redisconn= Redis(host="localhost", port= 6379,decode_responses=False)

logFileName = f'./logdata/'
try:
    if not os.path.exists(logFileName):
        os.makedirs(logFileName)
except Exception as e:
    print(e)  

humanTime= datetime.datetime.now()
todaydate=str(humanTime.date())
todaydate=todaydate.replace(':', '')
logFileName+=f'{todaydate}_logfile.log'
            
logging.basicConfig(level=logging.DEBUG, filename=logFileName,
        format="[%(levelname)s]: %(message)s")

while True:
    time.sleep(0.2)
    order_response=redisconn.lpop("orderResponse")
    if order_response:
        order_response=json.loads(order_response)
        logging.info(f"{datetime.datetime.now()} {order_response}")
        
        try:
            raw_order = orderDB.find_one({'order_id':order_response['orderReference']})
        except Exception as e:
            logging.error(e)
            
        
        if not raw_order:
            logging.error(f"{datetime.datetime.now()} order_id {order_response['orderReference']} not found in mongoDB")
            logging.error(f'{datetime.datetime.now()} Pushing order back to redis key -> orderResponse')
            redisconn.rpush("orderResponse",json.dumps(order_response))
            continue
        
        if order_response['orderStatus'] in ['Requested','Ordered']:
            pass
        elif order_response['orderStatus']=='Cancelled': 
            orderDB.update_one({'order_id': order_response['orderReference']},
                    {"$set": {"filled": True,'filledPrice': 0}})
            logging.error(f"{datetime.datetime.now()} order cancelled, setting filled =True order_id {order_response['orderReference']}")
        elif order_response['orderStatus']=='Executed':
            filledPrice= float(order_response['cancelFlag'])/100
            orderDB.update_one({'order_id': order_response['orderReference']},
                    {"$set": {"filled": True,'filledPrice': filledPrice}})
            logging.info(f"{datetime.datetime.now()} order filled, setting filled =True order_id {order_response['orderReference']}")
            updatePosition(algoName=raw_order["algoName"],symbol=raw_order["symbol"],quantity=raw_order["quantity"],action=raw_order["action"])      
        else:
            logging.fatal(f"{datetime.datetime.now()} New  type of orderStatus")
            
       



