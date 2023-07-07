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


def getCurrentExpiry():  
    testDate = datetime.datetime.fromtimestamp(time.time())
    date = int(testDate.strftime('%d'))
    month = int(testDate.strftime('%m'))
    year = int(testDate.strftime('%Y'))
    if datetime.date(year, month, date) <= datetime.date(2023,5,4):
        return '04MAY23'
    if datetime.date(year, month, date) <= datetime.date(2023,5,11):
        return '11MAY23'
    if datetime.date(year, month, date) <= datetime.date(2023,5,18):
        return '18MAY23'
    if datetime.date(year, month, date) <= datetime.date(2023,5,25):
        return '25MAY23'   
    if datetime.date(year, month, date) <= datetime.date(2023,6,1):
        return '01JUN23'              
    if datetime.date(year, month, date) <= datetime.date(2023,6,8):
        return '08JUN23'
    if datetime.date(year, month, date) <= datetime.date(2023,6,15):
        return '15JUN23'
    if datetime.date(year, month, date) <= datetime.date(2023,6,22):
        return '22JUN23'
    if datetime.date(year, month, date) <= datetime.date(2023,6,28):
        return '28JUN23'
    if datetime.date(year, month, date) <= datetime.date(2023,7,6):
        return '06JUL23'
    if datetime.date(year, month, date) <= datetime.date(2023,7,13):
        return '13JUL23'
    raise Exception(f'Expiry not found, add data!')
