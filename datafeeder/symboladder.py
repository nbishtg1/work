import numpy as np
import pandas as pd
import time
from breeze_connect import BreezeConnect
import time
import redis
from configparser import ConfigParser
import json

configReader1 = ConfigParser()
configReader1.read('config.ini')

configReader = ConfigParser()
configReader.read('/root/work/config.ini')

token = configReader.get('userDetail', r'token')
# api_key = configReader.get('userDetail', r'api_key')
# api_secret= configReader.get('userDetail', r'api_secret')


data = configReader1.get('optionSymbols', r'symbolParameters') # gets the optionSymbols list 
data = json.loads(data)


breeze = BreezeConnect(api_key='77227i755v8b97V^9V22t705n9@Z6968')
breeze.generate_session(api_secret='33R66117N2P1EG19OV9v21109t3&87)B',session_token=token)
breeze.ws_connect()

MonthMap= {"JAN":"Jan","FEB":"Feb","MAR":"Mar",
           "APR":"Apr","MAY":"May","JUN":"Jun",
           "JUL":"Jul","AUG":"Aug","SEP":"Sep",
           "OCT":"Oct","NOV":"Nov","DEC":"Dec"} 




optionList=[]
idMap={}

idMap["4.1!NIFTY BANK"]="NIFTY BANK"
idMap["4.1!NIFTY 50"]="NIFTY 50"
for sym in data: # if the parameter 'Add' is true then the parameter is selected  
    if sym['Add'] == 'True':
        if sym["Base"]=="NIFTY":
            stock_code="NIFTY"
        elif sym["Base"]=="BANKNIFTY":
            stock_code="CNXBAN"
        
        expiry=sym["Expiry"]
        expiry_date = expiry[0:2] + "-" + MonthMap[expiry[2:5]] + "-" + "20"+ expiry[5:]    
        
        up=int(sym["Up"])
        down=int(sym["Down"])
        strikeDist=int(sym["StrikeDist"])
        for i in range(down,up+strikeDist,strikeDist):
            try:
                z=breeze.get_stock_token_value(exchange_code="NFO",
                                        stock_code=stock_code, 
                                        product_type="options", 
                                        expiry_date=expiry_date, 
                                        strike_price=str(i), 
                                        right="call", 
                                        get_exchange_quotes=True, 
                                        get_market_depth=False)
                
                optionList.append(z[0])    
                idMap[z[0]]= sym["Base"] + sym["Expiry"] + str(i) + "CE"
            except Exception as err:
                print(f'{err} for {sym["Base"] + sym["Expiry"] + str(i) + "CE"}')
            
            try:
                z=breeze.get_stock_token_value(exchange_code="NFO",
                                        stock_code=stock_code, 
                                        product_type="options", 
                                        expiry_date=expiry_date, 
                                        strike_price=str(i), 
                                        right="put", 
                                        get_exchange_quotes=True, 
                                        get_market_depth=False)
                optionList.append(z[0])    
                idMap[z[0]]= sym["Base"] + sym["Expiry"] + str(i) + "PE"
            except Exception as err:
                print(f'{err} for {sym["Base"] + sym["Expiry"] + str(i) + "PE"}')


with open('idMapforDay.json', 'w') as f:
    json.dump(idMap, f)

with open('subscriptionList.json', 'w') as f:
    json.dump(optionList, f)