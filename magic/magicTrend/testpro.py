import pymongo
from redis import Redis
import json
import time
import datetime
def statusMessage(mongo= None, redis = None, algoName = '',action = '',):
    if mongo != None:
        livedb = mongo['algo_status']['algo_status']
        statusdb = mongo['algo_status']['algo_message']
        if redis != None:
            if algoName != '':    
                if action != '':
                    if action in ['Start', 'Stop']:
                        status = 'On' if action == 'Start' else 'Off'
                        try:
                            livedb.update_one({'algoName':algoName}, {'$set':{'status':status}})
                            # redis.set(algoName, json.dumps({'action':action}))
                            statusdb.insert_one({'algoName':algoName,'type':'status','message':f'status changed to {status}','time_stamp':str(datetime.datetime.now())})
                            redis.set('lastOrderUpdated',json.loads(str(time.time())))
                            redis.rpush('alarms-0','monitor'.encode('utf-8'))
                            return f'{algoName},status changes to {status}'
                        except Exception as error:
                            return f'{error} in {algoName},{action} request'
                else:
                    return f'error : action should not be NoneType'
            else:
                return f'error: algoName should not be NoneType'
        else:
            return f'error: redis should not be NoneType'
    else:
        return f'error: mongo should not be NoneType'
        
                    
    
def infoMessage(mongo = None, redis = None, message = '', algoName = ''):
    
    if mongo != None:
        statusdb = mongo['algo_status']['algo_message']
        if algoName != '':
            try:
                statusdb.insert_one({'algoName':algoName,'type':'info','message':message,'time_stamp':str(datetime.datetime.now())})
                redis.set('lastOrderUpdated',json.loads(str(time.time())))
                redis.rpush('alarms-0','monitor'.encode('utf-8'))
                return True
            except Exception as error:
                return f'{error} in {algoName},{message} request' 
        else:
            return f'error: algoName should not be NoneType'
    else:
        return f'error: mongo should not be NoneType'
    
        
    
def errorMessage(mongo = None, redis = None, algoName = '',message = ''):
    if mongo != None:
        livedb = mongo['algo_status']['algo_status']
        statusdb = mongo['algo_status']['algo_message']
        if redis != None:
            if algoName != '':
                
                    try:
                        livedb.update_one({'algoName':algoName}, {'$set':{'status':'Off'}})
                        statusdb.insert_one({'algoName':algoName,'type':'error','message':message,'time_stamp':str(datetime.datetime.now())})
                        redis.set('lastOrderUpdated',json.loads(str(time.time())))
                        redis.rpush('alarms-0','algo-stop'.encode('utf-8'))
                        return True
                    except Exception as error:
                        return f'{error} in {algoName},{message} request'
                
            else:
                return f'error: algoName should not be NoneType'
        else:
            return f'error: redis should not be NoneType'
    else:
        return f'error: mongo should not be NoneType'
        
                