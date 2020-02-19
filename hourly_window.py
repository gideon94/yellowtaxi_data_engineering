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

current_window_time=0
current_window=0
current_window_records=defaultdict(int)
next_window_records=defaultdict(int)
count_next_window_records=0
publisher=Publisher()
EXIT=False

class ThreadSubscriber(Subscriber):
    def subscribe(self, args):
        (destination, name, listener)=args
        self.conn.set_listener(name, listener)
        self.conn.subscribe(destination=destination, id=name, ack='auto')


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

    # pickup_time=datetime.datetime.strptime(message['tpep_pickup_datetime'], '%Y-%m-%d %H:%M:%S')
    # drop_time=datetime.datetime.strptime(message['tpep_dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
    pickup_hour=int(message['tpep_pickup_datetime'][11:13])
    pickup_day=int(message['tpep_pickup_datetime'][8:10])
    drop_hour=int(message['tpep_dropoff_datetime'][11:13])
    drop_day=int(message['tpep_dropoff_datetime'][8:10])
    #check if pickup is in current window
    #if pickup_time.hour == current_window%24:
    if pickup_hour == current_window%24:
        #set time for window
        if current_window_time == 0:
            #current_window_time= '{:%Y-%m-%d %H:00:00}'.format(pickup_time)
            current_window_time= message['tpep_pickup_datetime'][0:13]+':00:00'
        current_window_records[message['PULocationID']]+=1

    #keep records only in current window or next window
    #check if pickup is in next window
    #elif ((((pickup_time.day-1)*24)+ pickup_time.hour - current_window)==1):
    elif ((((pickup_day-1)*24)+ pickup_hour - current_window)==1):
        count_next_window_records+=1
        next_window_records[message['PULocationID']]+=1
    
    #check if pickup is in current window
    #if drop_time.hour == current_window%24:
    if drop_hour == current_window%24:
        current_window_records[message['DOLocationID']]+=1
    
    #check if pickup is in next window
    #elif ((((drop_time.day-1)*24)+ drop_time.hour - current_window)==1):
    elif ((((drop_day-1)*24)+ drop_hour - current_window)==1):
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
        current_window_time=0
        gc.collect()


def main():
    global EXIT
    
    # subscription1=Subscriber()
    # subscription1.subscribe('/queue/source1', 'window_1', Listener(subscription1,action))
    # subscription2=Subscriber()
    # subscription2.subscribe('/queue/source2', 'window_2', Listener(subscription2,action))
    # subscription3=Subscriber()
    # subscription3.subscribe('/queue/source3', 'window_3', Listener(subscription3,action))
    # subscription4=Subscriber()
    # subscription4.subscribe('/queue/source4', 'window_4', Listener(subscription4,action))
    subscription1=ThreadSubscriber()
    subscription2=ThreadSubscriber()
    subscription3=ThreadSubscriber()
    subscription4=ThreadSubscriber()
    # subscribers=[('/queue/source1', 'window_1', Listener(subscription1,action)),
    # ('/queue/source2', 'window_2', Listener(subscription2,action)),
    # ('/queue/source3', 'window_3', Listener(subscription3,action)),
    # ('/queue/source4', 'window_4', Listener(subscription4,action))
    # ]

    processes=[]
    pool= Pool(processes=4)
    pool.map(subscription1.subscribe, [('/queue/source1', 'window_1', Listener(subscription1,action))])
    pool.map(subscription2.subscribe, [('/queue/source2', 'window_2', Listener(subscription2,action))])
    pool.map(subscription3.subscribe, [('/queue/source3', 'window_3', Listener(subscription3,action))])
    pool.map(subscription4.subscribe, [('/queue/source4', 'window_4', Listener(subscription4,action))])

    # for i in range(1,5):
    #     sub=Subscriber()
    #     process=Process(target=sub.subscribe,args=('/queue/source'+str(i), 'window_'+str(i), Listener(sub,action)))
    #     process.start()
    #     processes.append(process)

    while not EXIT:
        pass

    pool.close()
    pool.join()

    subscription1.disconnect()
    subscription2.disconnect()
    subscription3.disconnect()
    subscription4.disconnect()

if __name__ == '__main__':
    main()