import gc
import json
import stomp
import datetime
import pandas as pd
from collections import defaultdict
from collections import Counter
from listener import Listener
from publisher import Publisher
from subscriber import Subscriber
import threading

current_window_time=None
current_window=0
current_window_records=defaultdict(int)
next_window_records=defaultdict(int)
count_next_window_records=0
publisher=Publisher()
EXIT=False

def publish_frequency(msg):
    global publisher
    publisher.publish(json.dumps(msg), '/queue/analytics/hour')

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
        publisher.publish('exit','/queue/analytics/hour')
        publisher.disconnect()
        EXIT=True
        return

    pickup_time=datetime.datetime.strptime(message['tpep_pickup_datetime'], '%Y-%m-%d %H:%M:%S')
    drop_time=datetime.datetime.strptime(message['tpep_dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
    #check if pickup is in current window
    if pickup_time.hour == current_window%24:
        #set time for window
        if current_window_time is None:
            current_window_time= '{:%Y-%m-%d %H:00:00}'.format(pickup_time)
        current_window_records[message['PULocationID']]+=1

    #keep records only in current window or next window
    #check if pickup is in next window
    elif ((((pickup_time.day-1)*24)+ pickup_time.hour - current_window)==1):
        count_next_window_records+=1
        next_window_records[message['PULocationID']]+=1
    
    #check if pickup is in current window
    if drop_time.hour == current_window%24:
        current_window_records[message['DOLocationID']]+=1
    
    #check if pickup is in next window
    elif ((((drop_time.day-1)*24)+ drop_time.hour - current_window)==1):
        next_window_records[message['DOLocationID']]+=1

    #if 500 records are from the next window 
    #grace time for out of order records
    if (count_next_window_records>=500):
        msg={'window':current_window, 'zones':current_window_records, 'time':current_window_time}
        publish_frequency(msg)
        current_window_records=next_window_records
        next_window_records=defaultdict(int)
        count_next_window_records=0
        current_window+=1
        current_window_time=None
        gc.collect()


def main():
    global EXIT
    subscription=Subscriber()
    subscription.subscribe('/queue/source', 'window', Listener(subscription,action))
    threads = []
    for i in range(4):
        t = threading.Thread(target=action)
        threads.append(t)
        t.start()
    while not EXIT:
        pass
    subscription.disconnect()

if __name__ == '__main__':
    main()