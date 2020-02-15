import gc
import json
import stomp
import datetime
import pandas as pd
from collections import defaultdict
from collections import Counter
from .listener import Listener
from .publisher import Publisher
from .subscriber import Subscriber


current_window=1
current_window_records=defaultdict(int)
next_window_records=defaultdict(int)
count_next_window_records=0
publisher=Publisher()

def publish_frequency(msg):
    global publisher
    publisher.publish(json.dumps(msg), '/queue/analytics/hour')
    print(msg)


def action(message):
    global current_window
    global current_window_records
    global next_window_records
    global count_next_window_records


    pickup_time=datetime.datetime.strptime(message['tpep_pickup_datetime'], '%d-%m-%Y %H:%M:%S')
    drop_time=datetime.datetime.strptime(message['tpep_dropoff_datetime'], '%d-%m-%Y %H:%M:%S')

    #check if pickup is in current window
    if pickup_time.hour == current_window:
        current_window_records[message['PULocationID']]+=1

    #keep records only in current window or next window
    #check if pickup is in next window
    elif ((((pickup_time.day-1)*24)+ pickup_time.hour - current_window)==1):
        count_next_window_records+=1
        next_window_records[message['PULocationID']]+=1
    
    #check if pickup is in current window
    if drop_time.hour == current_window:
        current_window_records[message['DOLocationID']]+=1
    
    #check if pickup is in next window
    elif ((((drop_time.day-1)*24)+ drop_time.hour - current_window)==1):
        next_window_records[message['DOLocationID']]+=1

    #if 100 records are from the next window 
    #grace time for out of order records
    if (count_next_window_records>=100):
        msg={'window':current_window, 'zones':current_window_records}
        publish_frequency(current_window)
        current_window_records=next_window_records
        next_window_records=defaultdict(int)
        count_next_window_records=0
        current_window+=1
        gc.collect()

def main():
    subscription=Subscriber()
    subscription.subscribe('/queue/preprocess/cleanup', 2, Listener(subscription,action))

if __name__ == '__main__':
    main()