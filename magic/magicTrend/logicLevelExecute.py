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
import numpy as np
# import statusUpdater
from pymongo import MongoClient
from symphonyToolsRedis import (
    dataConnect, dataFetcher, findSymWithCondition, getCurrentExpiry, getNextExpiry,
    getMonthlyExpiry, getSymDelta, reconnect, symphonyIdFetcher)
from priceFinder import getSym,getSymbyPrice
from testpro import infoMessage, errorMessage, statusMessage
from ta import trend


class algoLogic:
    
    limitTime, extraPercent, timeLimit = None, None, None
    upperPriceLimitPercent = None
    lowerPriceLimitPercent = None
    idMap={}
    symListConn=None
    isLive=None
    tradeCount=0
    postOrderConn = None
    algoName = None
    
    openPnl = pd.DataFrame(
            columns=['Key','Symbol', 'EntryPrice', 'CurrentPrice',
                    'Quantity',
                    'PositionStatus', 'Pnl'])

    closedPnl = pd.DataFrame(
                columns=['Key','ExitTime','Symbol', 'EntryPrice', 'ExitPrice',
                        'Quantity','PositionStatus','Pnl','ExitType'])
    
    
    
    def makeDb(self, algoName, client, clientRedis):       
        try:
            self.conn = client
            print("Connected successfully to MongoDB!!!")
            # print(f'redis password--------------{redisPassword}')
            # self.redisConn = Redis(host = ip, password = redisPassword)
            # print("Connected successfully to Redis!!!")
            self.postOrderConn = clientRedis
            print(f'Connected to redis on localhost to publish orders!!!')

        except Exception as e:
            raise Exception(e)
        if self.isLive:
            dbName = f'%s' % (algoName+str(datetime.date.today()))
            db = self.conn[dbName]
            self.ordersCollec = db.orders
            dbNameBasket = f'BO_%s' % (algoName+str(datetime.date.today()))
            db = self.conn[dbNameBasket]
            self.ordersCollec_basket = db.orders
            # self.isLive = True
        logging.info(f'is Live {self.isLive}')
    
    
    def OHLCDataFetch(self,collectionMain):
            # time.sleep(0.5)
            cont = False
            OHLCMain = None

            dataCountMain = collectionMain.count_documents({})
            # print(dataCountMain)
            OHLCMain = pd.DataFrame(collectionMain.find().skip(dataCountMain - 800))

            OHLCMain.set_index('LastTradeTime', inplace=True)
            OHLCMain = (OHLCMain.reset_index().drop_duplicates(subset='LastTradeTime', keep='last').set_index('LastTradeTime').sort_index())
            return OHLCMain
        
    def date_expformat(self,currentTime):
        t=datetime.datetime.fromtimestamp(currentTime)
        #print(str(t.day)+str(t.month)+str(t.year))
        monthMap = {'JAN':1, 'FEB':2,'MAR':3, 'APR':4, 
                            'MAY':5, 'JUN':6, 'JUL':7, 'AUG':8, 'SEP':9,'OCT':10,'NOV':11,'DEC':12}
        mon_keys= list(monthMap.keys())
        mon_value= list(monthMap.values())
        day_str= str(t.day)
        if len(day_str)==1:           
            day_str= str(0)+ day_str
        mon_str=mon_keys[mon_value.index(t.month)]
        year_str= str(t.year)[2:]
        return (str(day_str+mon_str+year_str))  

   
    def postOrderToDbLIMIT(self,exchangeSegment,exchangeInstrumentID,orderSide,
                                orderQuantity,limitPrice,upperPriceLimit,lowerPriceLimit,
                                timePeriod,extraPercent):
            if self.isLive:
                self.tradeCount+=1
                algoOrderNum = (os.getpid()*10)+self.tradeCount
                postData = {'exchangeSegment': exchangeSegment, 'exchangeInstrumentID':exchangeInstrumentID,
                            'productType': 'NRML', 'orderType': 'LIMIT', 'orderSide':orderSide,
                            'timeInForce': 'DAY', 'disclosedQuantity': 0,
                            'orderQuantity': orderQuantity, 'limitPrice': limitPrice,'stopPrice': 0,
                            'upperPriceLimit': upperPriceLimit,
                            'lowerPriceLimit': lowerPriceLimit,
                            'algoOrderNum': algoOrderNum,
                            'timePeriod': timePeriod,
                            'extraPercent':extraPercent}

                #Order publish to the channel - "<algoName>_<authenticationKey>"
                status = self.postOrderConn.publish(f'{self.algoName}',json.dumps(postData))
                print(self.algoName)
                logData(f'Limit order publish status is {status}')


    def postOrderToDbMARKET(self,exchangeSegment,exchangeInstrumentID,orderSide,
                                orderQuantity,limitPrice,upperPriceLimit,lowerPriceLimit,
                                timePeriod,extraPercent):
            if self.isLive:
                self.tradeCount+=1
                algoOrderNum = (os.getpid()*10)+self.tradeCount
                postData = {'exchangeSegment': exchangeSegment, 'exchangeInstrumentID':exchangeInstrumentID,
                            'productType': 'NRML', 'orderType': 'MARKET', 'orderSide':orderSide,
                            'timeInForce': 'DAY', 'disclosedQuantity': 0,
                            'orderQuantity': orderQuantity, 'limitPrice': limitPrice,'stopPrice': 0,
                            'upperPriceLimit': upperPriceLimit,
                            'lowerPriceLimit': lowerPriceLimit,
                            'algoOrderNum': algoOrderNum,
                            'timePeriod': timePeriod,
                            'extraPercent':extraPercent}

                #Order publish to the channel - "<algoName>_<authenticationKey>"
                status = self.postOrderConn.publish(f'{self.algoName}',json.dumps(postData))
                logData(f'Limit order publish status is {status}')

    
    def getCall(self,indexPrice, strikeDist,
                        symWithExpiry,otmFactor):
            remainder = indexPrice%strikeDist
            atm = indexPrice -remainder if remainder<=(strikeDist/2) \
                                        else (indexPrice -remainder + strikeDist)
            callSym = symWithExpiry+str(int(atm)+otmFactor) +'CE'
            return callSym

    
    def getPut(self,indexPrice, strikeDist,
                    symWithExpiry,otmFactor):
        
        remainder = indexPrice%strikeDist
        atm = indexPrice -remainder if remainder<=(strikeDist/2) \
                                    else (indexPrice -remainder + strikeDist)
        putSym = symWithExpiry+str(int(atm)-otmFactor) +'PE'
        return putSym
        
    
    def getBuyLimitPrice(self, ltp, extraPer):
        limitPriceExtra = round(ltp*extraPer, 1)
        limitPrice = limitPriceExtra + ltp
        return round(limitPrice,1)

    
    def getSellLimitPrice(self, ltp, extraPer):
        limitPriceExtra = round(ltp*extraPer, 1)
        limitPrice = ltp - limitPriceExtra
        return round(limitPrice,1)
    
    
    def entryOrder(self, data, symbol,quantity, entrySide):
        

        entryPrice = data[self.idMap[symbol]]

        self.currentKey = self.timeData

        positionSide = 1 if entrySide=='BUY' else -1
        
        if entrySide == 'SELL':            
            limitPrice = self.getSellLimitPrice(entryPrice, self.extraPercent)
         
        elif entrySide == 'BUY':             
            limitPrice = self.getBuyLimitPrice(entryPrice, 0.05)
        
                 
        self.postOrderToDbLIMIT(exchangeSegment='NSEFO',
                                    exchangeInstrumentID=self.idMap[symbol],
                                    orderSide=entrySide,orderQuantity=quantity,
                                    limitPrice=limitPrice,
                                    upperPriceLimit=self.upperPriceLimitPercent*limitPrice,
                                    lowerPriceLimit=self.lowerPriceLimitPercent*limitPrice,
                                    timePeriod=self.timeLimit,
                                    extraPercent=self.extraPercent)
        
        self.levelAdder(entryPrice, symbol,quantity,positionSide)

        return entryPrice
                
    
    def levelAdder(self, entryPrice,symbol,quantity, positionSide):

        self.openPnl.loc[len(self.openPnl)] = [datetime.datetime.fromtimestamp(self.timeData)]+[symbol] + [entryPrice] \
                                        + [entryPrice] +[quantity]+[positionSide] + [0]
             
                
    
    def pnlCalculator(self):
        
        if not self.openPnl.empty:
            self.openPnl['Pnl']=(self.openPnl['CurrentPrice']-self.openPnl['EntryPrice'])*self.openPnl['Quantity']*self.openPnl['PositionStatus']
                    
        if not self.closedPnl.empty:
            self.closedPnl['Pnl']=(self.closedPnl['ExitPrice']-self.closedPnl['EntryPrice'])*self.closedPnl['Quantity']*self.closedPnl['PositionStatus']
            
    def exitOrder(self,data,symbol,quantity,PositionStatus):
        sock = None
        exitPrice= data[self.idMap[symbol]]
        
        if PositionStatus == -1:
            orderSide='BUY'
            limitPrice = self.getBuyLimitPrice(exitPrice, self.extraPercent)
        elif PositionStatus == 1:
            orderSide='SELL'
            limitPrice = self.getSellLimitPrice(exitPrice, self.extraPercent)
        
        self.postOrderToDbLIMIT(exchangeSegment='NSEFO',
                    exchangeInstrumentID=self.idMap[symbol],
                    orderSide=orderSide,orderQuantity=quantity,
                    limitPrice=limitPrice,
                    upperPriceLimit=self.upperPriceLimitPercent*limitPrice,
                    lowerPriceLimit=self.lowerPriceLimitPercent*limitPrice,
                    timePeriod=self.timeLimit,
                    extraPercent=self.extraPercent)


    
    

        
    def mainLogic(self, indexName, baseSym, strikeDist, isLive, algoName, algoConn, pingConn, clientDataInfo, clientInfo, clientRedis, configReader, configReaderIP ):
    
        try:  
           
            proceed= True
            sock=None
            data=None
            client=None
            
            #assign global variable
            self.algoName = algoName 
            if isLive =="True":
                self.isLive = True      
            
            #read required variables for algo from config file (config.ini)
            configReader = ConfigParser()
            configReader.read('config.ini')
            
            timeframe = int(configReader.get('inputParameters', r'timeframe'))
            quantity = int(configReader.get('inputParameters', r'quantity'))
            target = float(configReader.get('inputParameters', r'target'))
            stoploss = float(configReader.get('inputParameters', r'stoploss'))
            otmFactor = int(configReader.get('inputParameters', r'otmFactor'))
            priceupperlimit = int(configReader.get('inputParameters', r'priceupperlimit'))
            pricelowerlimit = int(configReader.get('inputParameters', r'pricelowerlimit'))
            hedgePercent = float(configReader.get('inputParameters', r'hedgePercent'))         
                           
            self.upperPriceLimitPercent = float(configReader.get('inputParameters', r'upperPriceLimitPercent'))
            self.lowerPriceLimitPercent = float(configReader.get('inputParameters', r'lowerPriceLimitPercent'))
            self.timeLimit = int(configReader.get('inputParameters', r'timeLimitOrder'))
            self.extraPercent = float(configReader.get('inputParameters', r'extraPercent'))

            # make result folder if not made
            try:
                if not os.path.exists('./backtestResults'):
                    os.makedirs('./backtestResults')
            except Exception as e:
                print(e) 
                
            # create log file and its directory
            humanTime= datetime.datetime.now()
            st=str(humanTime.date())
            st=st.replace(':', '')
            logFileName = f'./logdata/'         
            try:
                if not os.path.exists(logFileName):
                    os.makedirs(logFileName)
            except Exception as e:
                print(e)  
            logFileName+=f'{st}ExecutionLog_{indexName}_logfile.log'
                       
            logging.basicConfig(level=logging.DEBUG, filename=logFileName,
                    format="[%(levelname)s]: %(message)s")
    

            #create mongoClient for infra Database(i.e client) and liveMarket Database(i.e clientData)
            client = MongoClient(host = clientInfo['mongoHost'],  port = int(clientInfo['mongoPort']), username = clientInfo['mongoName'], password = clientInfo['mongoPass'])
            clientData = MongoClient(host = clientDataInfo['mongoHost'],  port = int(clientDataInfo['mongoPort']), username = clientDataInfo['mongoName'], password = clientDataInfo['mongoPass'])           
            

            #set connection with infra Database
            self.makeDb(algoName, client, clientRedis)     
            
            
            #access collectin using liveMarket Database(i.e clientData)
            dbMain = clientData[f'OHLC_MINUTE_{timeframe}']
            collection = dbMain[indexName] 


            done=False
            while not done:
                time.sleep(1)
                humanTime = datetime.datetime.now()
                if humanTime >= datetime.datetime(humanTime.year,humanTime.month,humanTime.day,9,15,20):
                    done=True                   
                    algoEndTime = datetime.time(15,30,0)   
                                 
                    OHLCMain = self.OHLCDataFetch(collection)
                    lastEnt = OHLCMain.index[-1]
                    sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [indexName])
                    
                    try:
                        with open(f'last_Data.json', 'r') as openfile:
                            Data = json.load(openfile)
                            
                    except:
                        post = {
                                "CE_count":0,
                                "PE_count":0,
                                "trade" :True,                          
                                "lastday": str(humanTime.date())
                               }
                        Data = post
                       
                        with open(f'last_Data.json', "w") as outfile:
                            json.dump(Data, outfile)

                        with open(f'last_Data.json', 'r') as openfile:
                            Data = json.load(openfile)      
                     
                    writeloc = './backtestResults/'   
                    
                    if Data["lastday"] == str(humanTime.date()):    
                        try:
                            self.closedPnl= pd.read_csv(f'{writeloc}{st}_{indexName}closePosition.csv',index_col=0)                                                                   
                        except: 
                            logData("closedPnl csv issue")
                        trade=Data["trade"]
                    
                    if Data["lastday"] != str(humanTime.date()):  
                        Data["trade"]=True
                        trade=Data["trade"]

                    try:
                        self.openPnl= pd.read_csv(f'openPosition.csv',index_col=0)
                        if not self.openPnl.empty:
                           sym_list=list(self.openPnl["Symbol"]) 
                           sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, sym_list)                                                                   
                    except: 
                        logData("openPnl csv issue")
                    
                    CE_count=Data["CE_count"]
                    PE_count=Data["PE_count"]     
                    Data["lastday"] = str(humanTime.date())                         
                    
                    logData("..........................START........................")
                    
            
            while proceed:
                time.sleep(0.1)
                
                if self.idMap:
                    data = dataFetcher(sock, self.symListConn)
                    
                humanTime=datetime.datetime.now()
                timeData = time.time()
                self.timeData = timeData
                
                # -------------------------- Start Time ----------------------------------------
                if humanTime.time() >= datetime.time(9, 15,30) and humanTime.time() <= algoEndTime:
                    timeData = time.time()
                    self.timeData = timeData
                    
                    if not self.openPnl.empty:
                        for ind in self.openPnl.index:                    
                            self.openPnl.at[ind, 'CurrentPrice'] = data[self.idMap[self.openPnl['Symbol'][ind]]]
                                                         
                    OHLCMain = self.OHLCDataFetch(collection)
                    if OHLCMain.index[-1] >  lastEnt:
                        lastEnt = OHLCMain.index[-1]
                                         
                        if not self.openPnl.empty:  
                            closeHedgeCE=False
                            closeHedgePE=False                            
                            for index, row in self.openPnl.iterrows():
                                if row["PositionStatus"]==-1:
                                    if row["CurrentPrice"] < (target * row['EntryPrice']): 
                                        if "CE" in row["Symbol"]:
                                            if CE_count==2:
                                                closeHedgeCE=True  
                                        if "PE" in row["Symbol"]:
                                            if PE_count==2:
                                                closeHedgePE=True                       
                                        self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timeData)] +[row['Symbol']]+\
                                                                                            [row['EntryPrice']]+\
                                                                                            [row['CurrentPrice']]+\
                                                                                            [row['Quantity']]+[row['PositionStatus']]+\
                                                                                            [0]+['target hit']                             
                                        self.openPnl.drop(index,inplace=True)
                                        self.exitOrder(data=data , symbol=row['Symbol'] , quantity=row['Quantity'] , PositionStatus=row['PositionStatus'])
                                        infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'TARGET HIT {row["Symbol"]} closed')                                  
                                        print(f"{humanTime}...........target hit, closed {row['Symbol']} @{row['CurrentPrice']}")
                                        logData(f"{humanTime}..........target hit, closed {row['Symbol']} @{row['CurrentPrice']}") 
                                        
                                        if "CE" in row["Symbol"]:
                                            CE_count-=1 
                                            logData(CE_count)
                                        if "PE" in row["Symbol"]:
                                            PE_count-=1 
                                            logData(PE_count)

                                    elif row["CurrentPrice"] > (stoploss * row['EntryPrice']):
                                        if "CE" in row["Symbol"]:
                                            if CE_count==2:
                                                closeHedgeCE=True  
                                        if "PE" in row["Symbol"]:
                                            if PE_count==2:
                                                closeHedgePE=True                                 
                                        self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timeData)] +[row['Symbol']]+\
                                                                                            [row['EntryPrice']]+\
                                                                                            [row['CurrentPrice']]+\
                                                                                            [row['Quantity']]+[row['PositionStatus']]+\
                                                                                            [0]+['stoploss hit']                                    
                                        self.openPnl.drop(index,inplace=True) 
                                        self.exitOrder(data=data , symbol=row['Symbol'] , quantity=row['Quantity'] , PositionStatus=row['PositionStatus'])
                                        infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'STOPLOSS HIT {row["Symbol"]} closed')                 
                                        print(f"{humanTime}...........stoploss hit, closed {row['Symbol']} @{row['CurrentPrice']}")
                                        logData(f"{humanTime}...........stoploss hit, closed {row['Symbol']} @{row['CurrentPrice']}")         
                                          
                                        if "CE" in row["Symbol"]:
                                            CE_count-=1 
                                            logData(CE_count)
                                        if "PE" in row["Symbol"]:
                                            PE_count-=1 
                                            logData(PE_count)                 
                            self.openPnl.reset_index(inplace=True,drop=True)
                            
                            if closeHedgePE or closeHedgeCE:
                                time.sleep(5)
                                if not self.openPnl.empty:  
                                    for index, row in self.openPnl.iterrows():
                                        z=False
                                        if closeHedgeCE and ("CE" in row["Symbol"]) and row["PositionStatus"]==1:
                                            z=True
                                        if closeHedgePE and ("PE" in row["Symbol"]) and row["PositionStatus"]==1:
                                            z=True 
                                        if z:
                                            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timeData)] +[row['Symbol']]+\
                                                                                        [row['EntryPrice']]+\
                                                                                        [row['CurrentPrice']]+\
                                                                                        [row['Quantity']]+[row['PositionStatus']]+\
                                                                                        [0]+['close hedge']                             
                                            self.openPnl.drop(index,inplace=True)
                                            self.exitOrder(data=data , symbol=row['Symbol'] , quantity=row['Quantity'] , PositionStatus=row['PositionStatus'])
                                            infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'HEDGE {row["Symbol"]} closed')                                  
                                            print(f"{humanTime}...........hedge closed {row['Symbol']} @{row['CurrentPrice']}")
                                            logData(f"{humanTime}..........hedge closed {row['Symbol']} @{row['CurrentPrice']}")
                                    self.openPnl.reset_index(inplace=True,drop=True)        
                                
                        opClose = OHLCMain['Close'][lastEnt]
                        opOpen = OHLCMain['Open'][lastEnt]
                        opHigh = OHLCMain['High'][lastEnt]
                        opLow = OHLCMain['Low'][lastEnt]
                        candle= datetime.datetime.fromtimestamp(lastEnt)
              
                        logData(f"Candle:{candle} , Open:{opOpen} , High:{opHigh} , Low:{opLow} , Close:{opClose}")
                                                                  
        
                        if candle.time()>=datetime.time(15, 23, 0): 
                            if getCurrentExpiry()== self.date_expformat(timeData):                                            
                                if not self.openPnl.empty:  
                                    for index, row in self.openPnl.iterrows():
                                        z=False
                                        if baseSym=="BANKNIFTY":
                                            if row["Symbol"][9:16]==self.date_expformat(timeData):
                                                z=True
                                        if baseSym=="NIFTY":
                                            if row["Symbol"][5:12]==self.date_expformat(timeData):
                                                z=True
                                        if z:                
                                            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timeData)] +[row['Symbol']]+\
                                                                                        [row['EntryPrice']]+\
                                                                                        [row['CurrentPrice']]+\
                                                                                        [row['Quantity']]+[row['PositionStatus']]+\
                                                                                        [0]+["expiry over"]
                                            self.openPnl.drop(index,inplace=True)
                                            self.exitOrder(data=data , symbol=row['Symbol'] , quantity=row['Quantity'] , PositionStatus=row['PositionStatus'])
                                            infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'DAY OVER {row["Symbol"]} closed')               
                                            print(f'{humanTime} expiry settlement-----------closed {row["Symbol"]} position @{row["CurrentPrice"]}')
                                            logData(f'{humanTime} expiry settlement----------closed {row["Symbol"]} position @{row["CurrentPrice"]}')   
                                            
                                            if row["PositionStatus"]==-1:
                                                if "CE" in row["Symbol"]:
                                                    CE_count-=1 
                                                    logData(CE_count)
                                                if "PE" in row["Symbol"]:
                                                    PE_count-=1
                                                    logData(PE_count) 
                                                                 
                                    self.openPnl.reset_index(inplace=True,drop=True)
                            
                            dayOpenEpoch= int(datetime.datetime(humanTime.year, humanTime.month, humanTime.day,9,15,0).timestamp())
                            dayOpen = OHLCMain['Open'][dayOpenEpoch]  
                            if opClose>=dayOpen:
                                signal="long" 
                            elif opClose<dayOpen:
                                signal="short" 
                            logData(f"dayOpen:{dayOpen} , dayClose:{opClose} , signal:{signal}")    
                            
                            if trade:
                                trade=False
                                if signal=="short":
                                    if CE_count<2:
                                        if self.date_expformat(timeData)!=getCurrentExpiry():
                                            expiry = getCurrentExpiry()
                                        if self.date_expformat(timeData)==getCurrentExpiry():
                                            expiry = getNextExpiry()
                                        symWithExpiry= baseSym +expiry
                                        indexPrice = data[self.idMap[indexName]]
                                        
                                        callSym = self.getCall(indexPrice=indexPrice,strikeDist=strikeDist,symWithExpiry=symWithExpiry,otmFactor=0)                                                                       
                                        sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [callSym])
                                        Price=data[self.idMap[callSym]]*0.5
                                        remainder = Price%strikeDist
                                        otm = Price -remainder if remainder<=(strikeDist/2) \
                                                                    else (Price -remainder + strikeDist)
                                        otm= int(otm) 
                                        logData(f"atm 50%:{Price}")
                                        logData(f"otm:{otm}")  
                                        callSym = self.getCall(indexPrice=indexPrice,strikeDist=strikeDist,symWithExpiry=symWithExpiry,otmFactor=otm)                                                                       
                                        sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [callSym])
                                        if data[self.idMap[callSym]]>=pricelowerlimit:
                                            pass
                                        elif data[self.idMap[callSym]] < pricelowerlimit:
                                            callSym=getSymbyPrice(symSide="CE",baseSym=symWithExpiry,priceReq=pricelowerlimit,lesser_grater="grater",configReader=configReaderIP)
                                            sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [callSym]) 
                                        
                                        if CE_count==1:
                                            z=hedgePercent
                                            while True:                                           
                                                priceReq= z * data[self.idMap[callSym]]
                                                callHedge = getSym(symSide="CE",baseSym=symWithExpiry,priceReq=priceReq,configReader=configReaderIP)                                            
                                                if callHedge!="NotFound":
                                                    logData(f"hedge captured at {z*100}% value")
                                                    break 
                                                if z>=0.2:
                                                    break  
                                                z+=0.01                                  
                                            if callHedge!="NotFound":
                                                sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [callHedge])
                                                self.entryOrder(data=data,symbol=callHedge, quantity=quantity,entrySide="BUY")         
                                                print(f'{humanTime} ..................buy Hedge {callHedge}  @{data[self.idMap[callHedge]]}')
                                                logData(f'{humanTime} .................buy Hedge {callHedge}  @{data[self.idMap[callHedge]]}')
                                                infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'BUY HEDGE {callHedge}')
                                                time.sleep(5)
                                        
                                        data = dataFetcher(sock, self.symListConn)
                                        self.entryOrder(data=data,symbol=callSym, quantity=quantity,entrySide="SELL")
                                        print(f'{humanTime} ..................sold {callSym}  @{data[self.idMap[callSym]]}')
                                        logData(f'{humanTime} .................sold {callSym}  @{data[self.idMap[callSym]]}')
                                        infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'NEW ENTRY {callSym} SELL')
                                        
                                        CE_count+=1
                                        logData(CE_count)
                                                                    
                                if signal=="long":
                                    if PE_count<2:
                                        if self.date_expformat(timeData)!=getCurrentExpiry():
                                            expiry = getCurrentExpiry()
                                        if self.date_expformat(timeData)==getCurrentExpiry():
                                            expiry = getNextExpiry()
                                        symWithExpiry= baseSym +expiry
                                        indexPrice = data[self.idMap[indexName]]
                                        
                                        putSym = self.getPut(indexPrice=indexPrice,strikeDist=strikeDist,symWithExpiry=symWithExpiry,otmFactor=0)                                                                       
                                        sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [putSym])
                                        Price=data[self.idMap[putSym]]*0.5
                                        remainder = Price%strikeDist
                                        otm = Price -remainder if remainder<=(strikeDist/2) \
                                                                    else (Price -remainder + strikeDist)
                                        otm= int(otm)   
                                        logData(f"atm 50%:{Price}")
                                        logData(f"otm:{otm}")  
                                        putSym = self.getPut(indexPrice=indexPrice,strikeDist=strikeDist,symWithExpiry=symWithExpiry,otmFactor=otm)                                                                       
                                        sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [putSym])
                                        if data[self.idMap[putSym]]>=pricelowerlimit:
                                            pass
                                        elif data[self.idMap[putSym]] < pricelowerlimit:
                                            putSym=getSymbyPrice(symSide="PE",baseSym=symWithExpiry,priceReq=pricelowerlimit,lesser_grater="grater",configReader=configReaderIP)
                                            sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [putSym]) 
                                            
                                        if PE_count==1:
                                            z= hedgePercent
                                            while True:                                           
                                                priceReq= z * data[self.idMap[putSym]]
                                                putHedge = getSym(symSide="PE",baseSym=symWithExpiry,priceReq=priceReq,configReader=configReaderIP)                                        
                                                if putHedge!="NotFound":
                                                    logData(f"hedge captured at {z*100}% value")
                                                    break
                                                if z>=0.2:
                                                    break  
                                                z+=0.01   
                                            if putHedge!="NotFound":
                                                sock, data, self.idMap, self.symListConn = reconnect(sock, self.idMap, [putHedge])
                                                self.entryOrder(data=data,symbol=putHedge, quantity=quantity,entrySide="BUY")         
                                                print(f'{humanTime} ..................buy Hedge {putHedge}  @{data[self.idMap[putHedge]]}')
                                                logData(f'{humanTime} .................buy Hedge {putHedge}  @{data[self.idMap[putHedge]]}')
                                                infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'BUY HEDGE {putHedge}')
                                                time.sleep(5)
                                        
                                        data = dataFetcher(sock, self.symListConn)        
                                        self.entryOrder(data=data,symbol=putSym, quantity=quantity,entrySide="SELL")
                                        print(f'{humanTime} ..................sold {putSym}  @{data[self.idMap[putSym]]}')
                                        logData(f'{humanTime} .................sold {putSym}  @{data[self.idMap[putSym]]}')
                                        infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'NEW ENTRY {putSym} SELL')
                                        PE_count+=1
                                        logData(PE_count)
                                                                        
                        self.pnlCalculator()
                
                        writeloc = './backtestResults/'
                        self.closedPnl.to_csv(f'{writeloc}{st}_{indexName}closePosition.csv')
                        self.openPnl.to_csv(f'openPosition.csv')
                            
                        Data["CE_count"]= CE_count
                        Data["PE_count"]= PE_count
                        Data["trade"]=trade
                        with open(f'last_Data.json', "w") as outfile:
                            json.dump(Data, outfile)
                        
                        if candle.time()>=datetime.time(15,23,0):  
                            logData("............stop algo..............")
                            logData(f"CE_count:{CE_count} , PE_count:{PE_count}")
                            break
                                        
        except Exception as err:
            print(err)
            logData(err)
            errorMessage(redis=pingConn, mongo=client, algoName=algoName,message=f'Process Error :{err}')
            algoConn.set(algoName,json.dumps({'action':'Stop'}))
            logData(err)
