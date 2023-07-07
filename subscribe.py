import redis
import time

redisconn= redis.Redis(host="localhost", port= 6379,decode_responses=False)
redisconn.set("hi",55)
print(float(redisconn.get("NIFTY25MAY2318000PE")))
# pubObj = redisconn.pubsub(ignore_subscribe_messages=True)
# pubObj.subscribe("hii")
# while True:
#     time.sleep(0.1)
#     msg=pubObj.get_message()
#     if msg:
#         print(msg)