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
from priceFinder import getSym
from expirytools import getCurrentExpiry


class algoLogic:
    
    limitTime, extraPercent, timeLimit = None, None, None
    upperPriceLimitPercent = None
    lowerPriceLimitPercent = None
    idMap={}
    symListConn=None
    isLive=None
    tradeCount=0
    redisconn = None
    algoName = None
    timeData=None
    
    openPnl = pd.DataFrame(
            columns=['Key','Symbol', 'EntryPrice', 'CurrentPrice',
                    'Quantity',
                    'PositionStatus', 'Pnl'])

    closedPnl = pd.DataFrame(
                columns=['Key','ExitTime','Symbol', 'EntryPrice', 'ExitPrice',
                        'Quantity','PositionStatus','Pnl','ExitType'])
    
        
    def OHLCDataFetch(self,collectionMain):
            # time.sleep(0.5)
            cont = False
            OHLCMain = None

            dataCountMain = collectionMain.count_documents({})
             
            OHLCMain = pd.DataFrame(collectionMain.find().skip(dataCountMain - 2))

            OHLCMain.set_index('ti', inplace=True)
            OHLCMain = (OHLCMain.reset_index().drop_duplicates(subset='ti', keep='last').set_index('ti').sort_index())
            return OHLCMain
        

    def postOrderToDbLIMIT(self,symbol,action,quantity,limitPrice,algoName):
            if self.isLive:
                postData = { 'symbol':symbol,
                             'action':action,
                            'quantity':quantity,
                            'limitPrice':limitPrice,
                            'algoName':self.algoName
                            }

                #Order publish to the channel - "<algoName>_<authenticationKey>"
                status = self.redisconn.publish(f'algo_order',json.dumps(postData))
                print(self.algoName)
                logData(f'Limit order publish status is {status}')
    
    def getSymbolByStrike(self,symWithExpiry,strike,side):
        symbol= symWithExpiry+str(strike)+side
        return symbol
            
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
    
    
    def getBuyLimitPrice(self, ltp, extraPer):
        limitPriceExtra = round(ltp*extraPer, 1)
        limitPrice = limitPriceExtra + ltp
        return round(limitPrice,1)

    
    
    def getSellLimitPrice(self, ltp, extraPer):
        limitPriceExtra = round(ltp*extraPer, 1)
        limitPrice = ltp - limitPriceExtra
        return round(limitPrice,1)
    
    
    def entryOrder(self, data, symbol,quantity, entrySide):
        

        entryPrice = data[symbol]
       
        self.extraPercent= 3/entryPrice
       
        self.currentKey = self.timeData

        positionSide = 1 if entrySide=='BUY' else -1
        
        if entrySide == 'SELL':            
            limitPrice = self.getSellLimitPrice(entryPrice, self.extraPercent)
         
        elif entrySide == 'BUY':             
            limitPrice = self.getBuyLimitPrice(entryPrice, self.extraPercent)
        
        if limitPrice<=0:
            limitPrice=0.1
                 
        self.postOrderToDbLIMIT(symbol=symbol,
                                action=entrySide,
                                quantity=quantity,
                                limitPrice=limitPrice,
                                algoName=self.algoName)
        
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
        exitPrice= data[symbol]
        
        self.extraPercent= 3/exitPrice

        if PositionStatus == -1:
            orderSide='BUY'
            limitPrice = self.getBuyLimitPrice(exitPrice, self.extraPercent)
        elif PositionStatus == 1:
            orderSide='SELL'
            limitPrice = self.getSellLimitPrice(exitPrice, self.extraPercent)
        
        if limitPrice<=0:
            limitPrice=0.1

        self.postOrderToDbLIMIT(symbol=symbol,
                                action=orderSide,
                                quantity=quantity,
                                limitPrice=limitPrice,
                                algoName=self.algoName)


    
    def mainLogic(self, indexName, baseSym, strikeDist, isLive, algoName ):
        # try: 
            humanTime= datetime.datetime.now()
            # if self.date_expformat(time.time())==getCurrentExpiry():
            if True:  
                proceed= True
                sock=None
                data=None
                client=None
                lt=[]
                data={}
                
                self.algoName=algoName
                self.redisconn = Redis(host="localhost", port= 6379,decode_responses=True)
                client = MongoClient("mongodb://localhost:27017")
                
                self.algoName = algoName
                if isLive =="True":
                    self.isLive = True         
                
                configReader = ConfigParser()
                configReader.read('config.ini')
                     
                quantity = int(configReader.get('inputParameters', r'quantity'))
                buffer = float(configReader.get('inputParameters', r'buffer'))
                timeframe = int(configReader.get('inputParameters', r'timeframe'))
                
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
                    logData(e)
                
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
                print(logFileName)        
                logging.basicConfig(level=logging.DEBUG, filename=logFileName,
                        format="[%(levelname)s]: %(message)s")
                
            
                
                dbMain = client[f'OHLC_minute_{timeframe}']
                collection = dbMain[indexName]
                
                       
                done=False
                while not done:   
                    time.sleep(0.1)
                    humanTime = datetime.datetime.now()
                    if humanTime.time() >= datetime.time(9,15,20):
                        done=True                   
                        algoEndTime = datetime.time(15,30,0)   
                                  
                        OHLCMain = self.OHLCDataFetch(collection)
                        lastEnt = OHLCMain.index[-1]
                        
                        try:
                            with open(f'last_Data.json', 'r') as openfile:
                                Data = json.load(openfile)
                                
                        except:
                            post = {                              
                                    "lastday" : str(humanTime.date()),
                                    "trade" : True,
                                    "lt" : []       
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
                                self.openPnl= pd.read_csv(f'{writeloc}{st}_{indexName}openPosition.csv',index_col=0)                                                                                        
                            except: 
                                logData("closedPnl and openPnl csv issue")
                            trade=Data["trade"]
                            lt=Data["lt"] 
                                           
                        if Data["lastday"] != str(humanTime.date()): 
                            Data["trade"]=True
                            trade= Data["trade"]                                            
                            
                        Data["lastday"] = str(humanTime.date())                         
                        key=indexName+"lastEnt"
                        logging.info("..........................START........................")
                        
                
                while proceed:
                    time.sleep(0.2)
                    
                    humanTime=datetime.datetime.now()
                    timeData = time.time()
                    self.timeData = timeData
                    
                    # -------------------------- Start Time ----------------------------------------
                    
                        
                    expiry = getCurrentExpiry()
                    symWithExpiry= baseSym + expiry
                    
                    # if int(self.redisconn.get(key))>lastEnt:                        
                    OHLCMain = self.OHLCDataFetch(collection)
                    if OHLCMain.index[-1] >  lastEnt:
                        lastEnt = OHLCMain.index[-1] 
                        
                        opClose = OHLCMain['close'][lastEnt]
                        opOpen = OHLCMain['open'][lastEnt]
                        opHigh = OHLCMain['high'][lastEnt]
                        opLow = OHLCMain['low'][lastEnt]
                        candle= datetime.datetime.fromtimestamp(lastEnt)       
                        logData(f"Candle:{candle} , Open:{opOpen} , High:{opHigh} , Low:{opLow} , Close:{opClose}")
                        indexPrice=opClose
                        
                        if candle.time()>=datetime.time(9,29,0): 
                                                    
                            if not self.openPnl.empty:                       
                                #update current price in openPnl
                                for ind in self.openPnl.index:                    
                                    self.openPnl.at[ind, 'CurrentPrice'] = float(self.redisconn.get(self.openPnl['Symbol'][ind]))
                                                
                            # if data:
                            #     for i in data.keys():
                            #         data[i]=float(self.redisconn.get(i))
                            
                            if trade:                             
                                remainder = indexPrice%strikeDist
                                atm = indexPrice -remainder if remainder<=(strikeDist/2) \
                                                            else (indexPrice -remainder + strikeDist)
                                atm=int(atm)
                                if atm<indexPrice:
                                    lt=[atm,(atm+strikeDist)]
                                if atm>=indexPrice:
                                    lt=[(atm-strikeDist),atm]
                                for i in lt:
                                    putSym=self.getSymbolByStrike(symWithExpiry=symWithExpiry,strike=int(i),side="PE")    
                                        
                                    data[putSym]=float(self.redisconn.get(putSym))               
                                        
                                    self.entryOrder(data=data,symbol=putSym, quantity=quantity,entrySide="SELL")                                                                                                      
                                    print(f"sell {putSym} {humanTime}")
                                    logData(f'{humanTime}-------------built {putSym} short position')
                                    
                                    
                                    callSym=self.getSymbolByStrike(symWithExpiry=symWithExpiry,strike=int(i),side="CE")
                                    data[callSym]=float(self.redisconn.get(callSym))
                                    self.entryOrder(data=data,symbol=callSym, quantity=quantity,entrySide="SELL")        
                                    print(f"sell {callSym} {humanTime}")
                                    logData(f'{humanTime}-------------built {callSym} short position ')  
                                    
                                trade=False
                                logData(f"lt:{lt}")
                        
                            if indexPrice < (lt[0]-buffer):    
                                if not self.openPnl.empty:         
                                    for index, row in self.openPnl.iterrows():
                                        if str(lt[1]) in row["Symbol"]:
                                            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timeData)] +[row['Symbol']]+\
                                                                                        [row['EntryPrice']]+\
                                                                                        [row['CurrentPrice']]+\
                                                                                        [row['Quantity']]+[row['PositionStatus']]+\
                                                                                        [0]+["shift straddle downward"]
                                            self.openPnl.drop(index,inplace=True)
                                            data[row['Symbol']]=float(self.redisconn.get(row['Symbol']))
                                            self.exitOrder(data=data , symbol=row['Symbol'] , quantity=row['Quantity'] , PositionStatus=row['PositionStatus'])
                                            print(f'{humanTime} shift straddle downward-------------------closed {row["Symbol"]} short position @{row["CurrentPrice"]}')
                                            logData(f'{humanTime} shift straddle downward---------------------closed {row["Symbol"]} short position @{row["CurrentPrice"]}')
                                            
                                    self.openPnl.reset_index(inplace=True,drop=True)
                                    atm=lt[0] 
                                    lt=[atm-strikeDist,atm]
                                    strike= lt[0]
                                    time.sleep(4)
                                    
                                    putSym=self.getSymbolByStrike(symWithExpiry=symWithExpiry,strike=strike,side="PE")
                                    data[putSym]=float(self.redisconn.get(putSym))
                                    self.entryOrder(data=data,symbol=putSym, quantity=quantity,entrySide="SELL")                                                                                                      
                                    print(f"{humanTime} sell {putSym} ")
                                    logData(f'{humanTime}-------------built {putSym} short position ')
                                    
                                    callSym=self.getSymbolByStrike(symWithExpiry=symWithExpiry,strike=strike,side="CE")
                                    data[callSym]=float(self.redisconn.get(callSym))
                                    self.entryOrder(data=data,symbol=callSym, quantity=quantity,entrySide="SELL")        
                                    print(f"sell {callSym} {humanTime}")
                                    logData(f'{humanTime}-------------built {callSym} short position')                     
                                    logData(f"lt:{lt}")
                            
                            if indexPrice > (lt[1]+buffer):    
                                if not self.openPnl.empty:         
                                    for index, row in self.openPnl.iterrows():
                                        if str(lt[0]) in row["Symbol"]:
                                            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timeData)] +[row['Symbol']]+\
                                                                                        [row['EntryPrice']]+\
                                                                                        [row['CurrentPrice']]+\
                                                                                        [row['Quantity']]+[row['PositionStatus']]+\
                                                                                        [0]+["shift straddle upward"]
                                            self.openPnl.drop(index,inplace=True)
                                            data[row["Symbol"]]=float(self.redisconn.get(row["Symbol"]))
                                            self.exitOrder(data=data , symbol=row['Symbol'] , quantity=row['Quantity'] , PositionStatus=row['PositionStatus'])
                                            print(f'{humanTime} shift straddle upward----------------closed {row["Symbol"]} short position @{row["CurrentPrice"]}')
                                            logData(f'{humanTime} shift straddle upward----------------closed {row["Symbol"]} short position @{row["CurrentPrice"]}')
                                            
                                    self.openPnl.reset_index(inplace=True,drop=True)
                                    atm=lt[1] 
                                    lt=[atm,atm+strikeDist]
                                    strike= lt[1]
                                    time.sleep(4)
                                    
                                    putSym=self.getSymbolByStrike(symWithExpiry=symWithExpiry,strike=strike,side="PE")
                                    data[putSym]=float(self.redisconn.get(putSym))
                                    self.entryOrder(data=data,symbol=putSym, quantity=quantity,entrySide="SELL")                                                                                                      
                                    print(f"sell {putSym} {humanTime}")
                                    logData(f'{humanTime}-------------built {putSym} short position')
                                    
                                    callSym=self.getSymbolByStrike(symWithExpiry=symWithExpiry,strike=strike,side="CE")
                                    data[callSym]=float(self.redisconn.get(callSym))
                                    self.entryOrder(data=data,symbol=callSym, quantity=quantity,entrySide="SELL")        
                                    print(f"sell {callSym} {humanTime}")
                                    logData(f'{humanTime}-------------built {callSym} short position') 
                                                    
                                    logData(f"lt:{lt}")
                            
                            if candle.time()>=datetime.time(15,20,0):       
                                if not self.openPnl.empty:           
                                    for index, row in self.openPnl.iterrows():
                                        self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timeData)] +[row['Symbol']]+\
                                                                                    [row['EntryPrice']]+\
                                                                                    [row['CurrentPrice']]+\
                                                                                    [row['Quantity']]+[row['PositionStatus']]+\
                                                                                    [0]+["expiry over"]
                                        self.openPnl.drop(index,inplace=True)
                                        data[row["Symbol"]]=float(self.redisconn.get(row["Symbol"]))
                                        self.exitOrder(data=data , symbol=row['Symbol'] , quantity=row['Quantity'] , PositionStatus=row['PositionStatus'])
                                        print(f'{humanTime} expiry settlement-------------closed {row["Symbol"]} position @{row["CurrentPrice"]}')
                                        logData(f'{humanTime} expiry settlement-------------closed {row["Symbol"]} position @{row["CurrentPrice"]}')                            
                                        
                                    self.openPnl.reset_index(inplace=True,drop=True)
                                
                                
                                
                                
                            self.pnlCalculator()
                    
                            writeloc = './backtestResults/'
                            self.closedPnl.to_csv(f'{writeloc}{st}_{indexName}closePosition.csv')
                            self.openPnl.to_csv(f'{writeloc}{st}_{indexName}openPosition.csv')
                            
                            
                            Data["trade"]=trade
                            Data["lt"]=lt
                                                
                            with open(f'last_Data.json', "w") as outfile:
                                json.dump(Data, outfile)
                                
                            if candle.time()>=datetime.time(15,20,0):
                                logData("..............stop algo...............")
                                break                
            
        # except Exception as err:
        #     print(err)
        #     logData(err)
            
            
            
