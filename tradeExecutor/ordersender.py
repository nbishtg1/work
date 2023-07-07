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

def on_ticks(ticks):  
    # logging.info(f"{datetime.datetime.now()} {ticks}")
    redisconn.rpush("orderResponse",json.dumps(ticks))
    
breeze.on_ticks = on_ticks
breeze.subscribe_feeds(get_order_notification=True)


 
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

def get_option_details(symbol):
    if symbol.startswith("NIFTY"):
        stock_code= "NIFTY"
        expiry_date = symbol[5:7] + "-" + symbol[7:10] + "-" + "20"+ symbol[10:12]

        strike= symbol[12:-2]
        right= symbol[-2:]
        if right=="CE":
            right="call"
        elif right=="PE":
            right="put"   
    elif symbol.startswith("BANKNIFTY"):
        stock_code= "CNXBAN"
        expiry_date = symbol[9:11] + "-" + symbol[11:14] + "-" + "20"+ symbol[14:16]
        strike= symbol[16:-2]
        right= symbol[-2:]
        if right=="CE":
            right="call"
        elif right=="PE":
            right="put"    
    
    elif symbol.startswith("FINNIFTY"):
        stock_code= "NIFFIN"
        expiry_date = symbol[8:10] + "-" + symbol[10:13] + "-" + "20"+ symbol[13:15]

        strike= symbol[15:-2]
        right= symbol[-2:]
        if right=="CE":
            right="call"
        elif right=="PE":
            right="put"   
    return stock_code , expiry_date , strike , right

def placeorder(symbol,action,quantity,limitPrice):
    
    stock_code , expiry_date , strike , right =  get_option_details(symbol)
    logging.info(f"{datetime.datetime.now()} order details sending to broker-> {stock_code} {expiry_date} {strike} {right} , {action} , {limitPrice} ")
    z=breeze.place_order(stock_code=stock_code,
                    exchange_code="NFO",
                    product="options",
                    action= action,
                    order_type="limit",
                    stoploss="",
                    quantity= quantity,
                    price=limitPrice,
                    validity="day",
                    disclosed_quantity="0",
                    expiry_date= expiry_date,
                    right= right,
                    strike_price= strike,
                    user_remark=12345678)
    return(z)



def rmsCheck(symbol,action,quantity,limitPrice,algoName):
    maxPriceLimit=200
    lotSize=50
    
    if quantity%lotSize==0:
        pass
    else:
        return [False,f"lotSize error {quantity}"]
       
    if action=="BUY" or action=="SELL":
        pass
    else:
        return [False, f"action other than BUY SELL i.e {action}"]
    
    if limitPrice < maxPriceLimit:
        pass
    else:
        return [False, f"limitPrice exceeds maxPricelimit of {maxPriceLimit}"]
    
    return [True, "all checks cleared"]
 
 
todaydate = datetime.date.today()

client = MongoClient("mongodb://localhost:27017")
orderDB = client["order"][f"order_{todaydate}"]
redisconn= Redis(host="localhost", port= 6379,decode_responses=False) 
    
pubObj = redisconn.pubsub(ignore_subscribe_messages=True)
pubObj.subscribe("algo_order")


while True:
    
    order = pubObj.get_message()       
    if order:
        order = json.loads(order['data']) 
    else:
        time.sleep(0.2)
    if order:    
        logging.info(f"{datetime.datetime.now()}: order received from algo: {order['algoName']}, now placing order")
        logging.info(f"{datetime.datetime.now()} {order}")     
        
        z=  rmsCheck(symbol=order["symbol"]    , action=order["action"],
                     quantity=order["quantity"], limitPrice=order["limitPrice"], algoName=order["algoName"])
        
        if z[0]==True:
            pass
        else:
            logging.error(f"{datetime.datetime.now()} RMS rejection due to {z[1]}")
            continue
        
        initial_response=None
        try:
            orderSentTime= time.time()
            initial_response=placeorder(symbol=order["symbol"]    , action=order["action"],
                                        quantity=order["quantity"], limitPrice=order["limitPrice"])
        except Exception as e:
            logging.error(e)
            
        if initial_response:
            logging.info(f"{datetime.datetime.now()}: initial order response received from broker ,delay by {round((time.time() - orderSentTime), 2)} seconds")
            try:
                if initial_response["Status"]== 200:
                    logging.info(f"{datetime.datetime.now()} {initial_response}")
                    order_id= initial_response['Success']['order_id'] 
                    order["order_id"]     = order_id
                    order["filled"]       = False
                    order["filledPrice"]  = 0                        
                    order["algoOrderTime"]= orderSentTime 
                    order["orderSentTime"]= orderSentTime 
                    order["modifyCount"]  = 0
                    
                    orderDB.insert_one(order)
                    logging.info(f"{datetime.datetime.now()} order Placed sucessfully, inserted order in mongoDB")
                    logging.info("..")
                    # print(type(order))
                    del order['_id']                 
                    redisconn.rpush("modifyOrder",json.dumps(order))
                elif initial_response["Status"]==500: 
                    logging.error(f"{datetime.datetime.now()} {initial_response}") 
                    
                    if "Limit is insufficient" in initial_response['Error']:
                        print("margin shortfall")
                    logging.info("..")
                else: 
                    logging.error(f"{datetime.datetime.now()} {initial_response}") 
                    logging.info("..")   
            except Exception as e:
                logging.fatal(f"{datetime.datetime.now()} {initial_response}")  
                logging.fatal(e) 
        
   
                     
             
                
                
     