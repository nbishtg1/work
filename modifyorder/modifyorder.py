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

configReader = ConfigParser()
configReader.read('/root/work/config.ini')

token = configReader.get('userDetail', r'token')
# api_key = configReader.get('userDetail', r'api_key')
# api_secret= configReader.get('userDetail', r'api_secret')

breeze = BreezeConnect(api_key='77227i755v8b97V^9V22t705n9@Z6968')
breeze.generate_session(api_secret='33R66117N2P1EG19OV9v21109t3&87)B',session_token=token)
breeze.ws_connect()
print("login sucessful")

def modifyorder(order_id,limitPrice):
    z=breeze.modify_order(order_id=order_id,
                        exchange_code="NFO",
                        order_type="limit",
                        stoploss="",
                        quantity="",
                        price=limitPrice,
                        validity="day",
                        disclosed_quantity="0")
    return(z)

client = MongoClient("mongodb://localhost:27017")
orderDB = client["order"][f"order_{todaydate}"]
redisconn= Redis(host="localhost", port= 6379,decode_responses=False)


while True: 
    time.sleep(0.2) 
    z=redisconn.lpop("modifyOrder")
    if z:
        z=json.loads(z)    
        if z["orderSentTime"] < (time.time()-6):
            order_id=z['order_id']
            raw_order = orderDB.find_one({'order_id':order_id})
            if raw_order["filled"]==True:
                logging.info(f"{datetime.datetime.now()} order_id {order_id} filled,removing from modifyOrder queue")
                logging.info("..")
            elif raw_order["filled"]==False:
                ltp= float(redisconn.get(z["symbol"]))
                action=z["action"]
                if action=="BUY":
                    limitPriceExtra = round(ltp*0.01, 1)
                else:
                    limitPriceExtra = -round(ltp*0.01, 1)
                limitPrice= ltp + limitPriceExtra
                
                logging.info(f'{datetime.datetime.now()} modifying order_id {z["order_id"]} with limitPrice @{limitPrice}')
                modify_response=modifyorder(order_id=z["order_id"],limitPrice=limitPrice)
                
                if modify_response['Status']==200:
                    logging.info(f"{datetime.datetime.now()} {modify_response}")
                    logging.info(f'{datetime.datetime.now()} order_id {z["order_id"]} modified sucessfully') 
                    orderDB.update_one({'order_id': order_id},
                                    {"$set": {"orderSentTime": time.time(),"modifyCount":z["modifyCount"]+1}})
                    z["orderSentTime"]=time.time()
                    z["modifyCount"]+=1
                    if z["modifyCount"]>=4:
                        logging.info(f"{datetime.datetime.now()} modified max times 4, removing order_id {order_id} from modifyOrder queue")
                    else:
                        redisconn.rpush("modifyOrder",json.dumps(z))
                    logging.info("..")
                elif modify_response['Status']==500:
                    logging.error(f"{datetime.datetime.now()} {modify_response}")              
                    if modify_response['Error']== 'Order Quantity is already executed.':
                        logging.info(f"{datetime.datetime.now()} order_id {order_id} filled,removing from modifyOrder queue") 
                    logging.info("..")                               
        else:
            redisconn.rpush("modifyOrder",json.dumps(z))
        
        
        
        
        
        
        
        
        
        
        
        
        

            