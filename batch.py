import gc
import json
import stomp
import datetime
import pandas as pd
import multiprocessing
from collections import defaultdict
from collections import Counter
from listener import Listener
from publisher import Publisher
from subscriber import Subscriber
from multiprocessing import Process
from multiprocessing.pool import ThreadPool as Pool

current_window_time='2018-01-01 00:00:00'
current_window=0
current_window_records=[]
next_window_records=[]
count_next_window_records=0
publisher=Publisher()
EXIT=False


def publish_frequency(msg):
    global publisher
    publisher.publish(json.dumps(msg), '/queue/batch')

def action(message):
    global current_window
    global current_window_records
    global next_window_records
    global count_next_window_records
    global current_window_time
    global EXIT

    if message=='exit':
        msg={'window':current_window, 'zones':current_window_records, 'time':current_window_time}
        publish_frequency(msg)
        current_window_time=current_window_time+datetime.timedelta(hours=1)
        msg={'window':current_window+1, 'zones':next_window_records, 'time':current_window_time}
        publish_frequency(msg)
        publisher.publish('exit','/queue/batch/hour')
        publisher.disconnect()
        EXIT=True
        return

    pickup_hour=int(message['tpep_pickup_datetime'][11:13])
    pickup_day=int(message['tpep_pickup_datetime'][8:10])
    drop_hour=int(message['tpep_dropoff_datetime'][11:13])
    drop_day=int(message['tpep_dropoff_datetime'][8:10])

    if pickup_hour == current_window%24:
        current_window_records.append(message['PULocationID'])

    #keep records only in current window or next window
    #check if pickup is in next window
    if ((((pickup_day-1)*24)+ pickup_hour - current_window)==1):
        count_next_window_records+=1
        next_window_records.append(message['DOLocationID'])
        
    #check if pickup is in current window
    if drop_hour == current_window%24:
        current_window_records.append(message['DOLocationID'])
    
    #check if pickup is in next window
    if ((((drop_day-1)*24)+ drop_hour - current_window)==1):
        next_window_records.append(message['DOLocationID'])

    #if 500 records are from the next window 
    #grace time for out of order records
    if(len(current_window_records)>3000):
        msg={'window':current_window, 'zones':current_window_records, 'time':current_window_time}
        current_window_records=[]
        publish_frequency(msg)


    if (len(next_window_records)>500):
        msg={'window':current_window, 'zones':current_window_records, 'time':current_window_time}
        publish_frequency(msg)
        current_window_records=next_window_records
        next_window_records=[]
        count_next_window_records=0
        current_window+=1
        current_window_time=message['tpep_pickup_datetime'][0:13]+':00:00'
        #gc.collect()


def main():
    global EXIT
    subscription=Subscriber()
    subscription.subscribe('/queue/source', 'batch', Listener(subscription,action))
    while not EXIT:
        pass
    subscription.disconnect()

if __name__ == '__main__':
    main()