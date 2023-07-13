import time
from redis import Redis
from multiprocessing import Process
from configparser import ConfigParser
from pymongo import MongoClient
import json
import datetime
from testpro import infoMessage, errorMessage, statusMessage
# import tradeExecutorLimitDirect
import newMockalgo
import os
import logging
# configfile = '../../RMS-git/config.ini'
# config = ConfigParser()
# config.read(configfile)


# livedb.insert_one({'algoName':algoName,'status':'Off'})




def getDatetimeParam(paramName, addDate, configReader):
    finalTime = configReader.get('inputParameters', f'{paramName}')
    finalTime = finalTime.split(':')
    finalDateTime = datetime.datetime(addDate.year,
                                            addDate.month, addDate.day,
                                            int(finalTime[0]),int(finalTime[1]),
                                            int(finalTime[2]))
    return finalDateTime.timestamp()

def run(configReader, isLive, algoName):
    
    algoConn = Redis(host = configReader['DBParams']['redisHost'], db = 8, password = configReader['DBParams']['redisPass'])
    pingConn = Redis(host = configReader['DBParams']['redisHost'], password = configReader['DBParams']['redisPass'],db = 9)
    objLevel = newMockalgo.algoLogic()
 


    inputParams = {'timeFrame' : int(configReader.get('inputParameters', f'timeFrame')),
                
                'isLive': configReader.get('inputParameters', f'isLive'), 
                'indexName': configReader.get('inputParameters', f'indexName'),
                'baseSym': configReader.get('inputParameters', f'baseSym'),

                'authenticationKey':1234,
                # 'limitTime': int(configReader.get('inputParameters', f'limitTime')),
                # 'extraPercent': float(configReader.get('inputParameters', f'extraPercent')),
                'algoName' : algoName,
                'algoConn': algoConn,
                'pingConn': pingConn
                }

    action = 'Start'
    algoName = algoName
    algoConn.set(algoName, json.dumps({'action':action}))
    client = MongoClient(host = configReader['DBParams']['mongoHost'],
                        port = int(configReader['DBParams']['mongoPort']), 
                        username = configReader['DBParams']['mongoName'], 
                        password = configReader['DBParams']['mongoPass'])
    livedb = client['algo_status']['algo_status']
    inputParams['mongoPassword'] = configReader['DBParams']['mongoPass']
    inputParams['mongoUsername'] = configReader['DBParams']['mongoName']

    inputParams['redisPassword'] = configReader['DBParams']['redisPass']

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
    
    mongoHost,redisHost = configReader['DBParams']['mongoHost'],configReader['DBParams']['redisHost']
    mongoPass, redisPass = configReader['DBParams']['mongoPass'],configReader['DBParams']['redisPass']
    mongoName = configReader['DBParams']['mongoName']
    algoName = configReader.get('inputParameters', f'algoName')
    start_date = datetime.datetime.now()
    print(algoName)
    isLive = configReader.get('inputParameters', r'isLive')
    # if liveCheck == 'True':
        # tradeExecutorLimitDirect.startOEMS(algoName, '1234',mongoHost=mongoHost,mongoName=mongoName, 
        # mongoPass=mongoPass,redisHost=redisHost,redisPass=redisPass)

    
    run(configReader, isLive, algoName)
    
    
    