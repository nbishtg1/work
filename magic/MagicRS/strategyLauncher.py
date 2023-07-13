import time
from redis import Redis
from multiprocessing import Process
from configparser import ConfigParser
from pymongo import MongoClient
import json
import datetime
from testpro import infoMessage, errorMessage, statusMessage
# import tradeExecutorLimitDirect
import logicLevelExecute
import os
import logging
import priceFinder


def getDatetimeParam(paramName, addDate, configReader):
    finalTime = configReader.get('inputParameters', f'{paramName}')
    finalTime = finalTime.split(':')
    finalDateTime = datetime.datetime(addDate.year,
                                            addDate.month, addDate.day,
                                            int(finalTime[0]),int(finalTime[1]),
                                            int(finalTime[2]))
    return finalDateTime.timestamp()

def run(configReader, configReaderIP, isLive, algoName):
    
    
    mongoHost,redisHost  = configReaderIP['InfraParams']['mongoHost'] , configReaderIP['InfraParams']['redisHost']
    mongoPass, redisPass = configReaderIP['InfraParams']['mongoPass'] , configReaderIP['InfraParams']['redisPass']
    mongoName, mongoPort = configReaderIP['InfraParams']['mongoName'] , configReaderIP['InfraParams']['mongoPort']

    mongoNameData, mongoPortData = configReaderIP['DBParams']['mongoName'] , configReaderIP['DBParams']['mongoPort']
    mongoHostData, mongoPassData = configReaderIP['DBParams']['mongoHost'] , configReaderIP['DBParams']['mongoPass']

    algoConn = Redis(host = redisHost, db = 8, password = redisPass)
    pingConn = Redis(host = redisHost, db = 9, password = redisPass)
    
    objLevel = logicLevelExecute.algoLogic()
 


    inputParams = { 'indexName' : configReader.get('inputParameters', f'indexName'),
                    'baseSym'   : configReader.get('inputParameters', f'baseSym'), 
                    'strikeDist': int(configReader.get('inputParameters', f'strikeDist')),          
                    'isLive'    : isLive,
                    'algoName'  : algoName,
                    'algoConn'  : algoConn,
                    'pingConn'  : pingConn
                    }

    action = 'Start'
    algoName = algoName
    algoConn.set(algoName, json.dumps({'action':action}))
    client = MongoClient(host = mongoHost,  port = int(mongoPort), username = mongoName, password = mongoPass)
    clientRedis = Redis(host = redisHost, password = redisPass)

    clientDataInfo = {'mongoHost':mongoHostData , 'mongoPort':mongoPortData , 'mongoName':mongoNameData , 'mongoPass':mongoPassData}
    clientInfo     = {'mongoHost':mongoHost     , 'mongoPort':mongoPort     , 'mongoName':mongoName     , 'mongoPass':mongoPass}
    
    inputParams['clientDataInfo'] = clientDataInfo
    inputParams['clientInfo'] = clientInfo
    inputParams['clientRedis'] = clientRedis
    
    inputParams['configReader'] = configReader
    inputParams['configReaderIP'] = configReaderIP


    livedb = client['algo_status']['algo_status']
    livedb.delete_one({'algoName':algoName})


    livedb.insert_one({'algoName':algoName,'status':'Off'})
    
    p1 = Process(target=objLevel.mainLogic, kwargs=inputParams)
    
    while True:
        time.sleep(1)
        while algoConn.get(algoName) != None:

            update = json.loads(algoConn.get(algoName))
            algoConn.delete(algoName)
            print(update)
            action = update['action']
            if action == 'Start':
                p1 = Process(target=objLevel.mainLogic, kwargs=inputParams)
                try:
                    p1.terminate()
                except:
                    pass
                print('start')
                p1.start()
                statusMessage(mongo= client, redis = pingConn, algoName = algoName, action = 'Start',)
                   
            elif action == 'Stop':
                p1.terminate()
                statusMessage(mongo= client, redis = pingConn, algoName = algoName, action = 'Stop',)
   
            else:
                infoMessage(mongo = client, redis = pingConn, algoName = algoName, message=f'incorrect action {action}')


if __name__ == '__main__':
    configReader = ConfigParser()
    configReader.read('config.ini')

    configReaderIP = ConfigParser()
    configReaderIP.read('/root/liveAlgos/dbconfig.ini')
    

    algoName = configReader.get('inputParameters', f'algoName')
    start_date = datetime.datetime.now()
    print(algoName)
    isLive = configReader.get('inputParameters', r'isLive')

    
    run(configReader, configReaderIP, isLive, algoName)
    
    
    
