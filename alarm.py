import datetime
import time
from playsound import playsound
import os

for i in range(50):
    time.sleep(1)
    if i%5==0:
        # playsound("ar.mp4")
        os.system("beep -f 2000 -l 1500")