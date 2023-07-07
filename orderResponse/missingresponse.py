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

configReader = ConfigParser()
configReader.read('/root/work/config.ini')

token = configReader.get('userDetail', r'token')

breeze = BreezeConnect(api_key='77227i755v8b97V^9V22t705n9@Z6968')
breeze.generate_session(api_secret='33R66117N2P1EG19OV9v21109t3&87)B',session_token=token)
breeze.ws_connect()

logFileName = f'./logdata/'
try:
    if not os.path.exists(logFileName):
        os.makedirs(logFileName)
except Exception as e:
    print(e)  

humanTime= datetime.datetime.now()
todaydate=str(humanTime.date())
todaydate=todaydate.replace(':', '')
logFileName+=f'{todaydate}_missingresponse.log'
            
logging.basicConfig(level=logging.DEBUG, filename=logFileName,
        format="[%(levelname)s]: %(message)s")

todaydate = datetime.date.today()
# todaydate="2023-06-28"
client = MongoClient("mongodb://localhost:27017")
orderDB = client["order"][f"order_{todaydate}"]

while True:
    try:
        unfilled_orders = list(orderDB.find({'filled':False}))
    except Exception as e:
        logging.error(e)
        print(e)
    
    if unfilled_orders:
        for i in unfilled_orders:
            if i["algoOrderTime"]< (time.time()-25):
                try:
                    logging.info(f"{datetime.datetime.now()} checking orderStatus")
                    requestSentTime=time.time()
                    response=breeze.get_order_detail(exchange_code="NFO",order_id=i["order_id"])
                except Exception as e:
                    logging.error(e)
                    print(e)
                if response:
                    if response["Status"]==200:             
                        logging.info(f"{datetime.datetime.now()} {response}")
                        logging.info(f"{datetime.datetime.now()} response delay {round(time.time()-requestSentTime,2)}")
                        if response["Success"][0]["status"]=="Executed":
                            order_id = response["Success"][0]['order_id']
                            filledPrice= float(response["Success"][0]['average_price'])                     
                            orderDB.update_one({'order_id': order_id},
                                        {"$set": {"filled": True,'filledPrice': filledPrice}})
                            logging.info(f"{datetime.datetime.now()} order filled ,setting filled =True") 
                        elif response["Success"][0]["status"]=="Cancelled":
                            order_id = response["Success"][0]['order_id']
                            filledPrice = 0                    
                            orderDB.update_one({'order_id': order_id},
                                        {"$set": {"filled": True,'filledPrice': filledPrice}})
                            logging.info(f"{datetime.datetime.now()} order cancelled ,setting filled =True") 
                        else:
                            logging.info(f"{datetime.datetime.now()} order yet not filled")
                        logging.info("..")  
                    elif response["Status"]==500:
                        logging.error(f"{datetime.datetime.now()} {response}")
                        logging.info("..")       
                    else:
                        logging.error(f"{datetime.datetime.now()} {response}")
                        logging.info("..")
    else:
        logging.info(f"{datetime.datetime.now()} no filled =False")
    time.sleep(25)




# response=breeze.get_order_detail(exchange_code="NFO",
#                         order_id="202306282400082882")

# if response["Status"]==200:
#     if response["Success"][0]["status"]=="Executed":
#         order_id = response["Success"][0]['order_id']
#         filledPrice= float(response["Success"][0]['average_price'])
        
#         orderDB.update_one({'order_id': order_id},
#                     {"$set": {"filled": True,'filledPrice': filledPrice}})
#         print(response)