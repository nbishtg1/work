import time
from redis import Redis
from multiprocessing import Process
from configparser import ConfigParser
from pymongo import MongoClient
import json
import datetime
# import tradeExecutorLimitDirect
import logicLevelExecute
import os
import logging
import priceFinder as priceFinder


def getDatetimeParam(paramName, addDate, configReader):
    finalTime = configReader.get('inputParameters', f'{paramName}')
    finalTime = finalTime.split(':')
    finalDateTime = datetime.datetime(addDate.year,
                                            addDate.month, addDate.day,
                                            int(finalTime[0]),int(finalTime[1]),
                                            int(finalTime[2]))
    return finalDateTime.timestamp()

def run(isLive, algoName):
    
    
    objLevel = logicLevelExecute.algoLogic()
 


    inputParams = { 'indexName' : configReader.get('inputParameters', f'indexName'),
                    'baseSym'   : configReader.get('inputParameters', f'baseSym'), 
                    'strikeDist': int(configReader.get('inputParameters', f'strikeDist')),          
                    'isLive'    : isLive,
                    'algoName'  : algoName,
                    }

    action = 'Start'
    # while True:
        # time.sleep(1)
    if action == 'Start':
        p1 = Process(target=objLevel.mainLogic, kwargs=inputParams)
        try:
            p1.terminate()
        except:
            pass
        print('start')
        p1.start()                        
    elif action == 'Stop':
        p1.terminate()
        print('stop')
    else:
        pass


if __name__ == '__main__':
    configReader = ConfigParser()
    configReader.read('config.ini')

    # configReaderIP = ConfigParser()
    # configReaderIP.read('/root/algos/dbconfig.ini')
    

    algoName = configReader.get('inputParameters', f'algoName')
    start_date = datetime.datetime.now()
    print(algoName)
    isLive = configReader.get('inputParameters', r'isLive')

    
    run(isLive, algoName)
    
    
    