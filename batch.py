import datetime
import json
import pandas as pd
import stomp
from listener import Listener
from publisher import Publisher
from subscriber import Subscriber

current_window_time = '2018-01-01 00:00:00'
current_window = 0
current_window_records = []
next_window_records = []
count_next_window_records = 0
publisher = Publisher()
EXIT = False

#publish the result
def publish(msg):
    global publisher
    publisher.publish(json.dumps(msg), '/queue/batch')

#action to be performed on message
def action(message):
    global current_window
    global current_window_records
    global next_window_records
    global count_next_window_records
    global current_window_time
    global EXIT

    #publish the pending message when exiting
    if message == 'exit':
        msg = {'window': current_window,
               'zones': current_window_records, 'time': current_window_time}
        publish(msg)
        current_window_time = current_window_time+datetime.timedelta(hours=1)
        msg = {'window': current_window+1,
               'zones': next_window_records, 'time': current_window_time}
        publish(msg)
        publisher.publish('exit', '/queue/batch/hour')
        publisher.disconnect()
        EXIT = True
        return

    #to perform comparison for windowing
    pickup_hour = int(message['tpep_pickup_datetime'][11:13])
    pickup_day = int(message['tpep_pickup_datetime'][8:10])
    drop_hour = int(message['tpep_dropoff_datetime'][11:13])
    drop_day = int(message['tpep_dropoff_datetime'][8:10])

    if pickup_hour == current_window % 24:
        current_window_records.append(message['PULocationID'])

    #keep records only in current window or next window
    #check if pickup is in next window
    if ((((pickup_day-1)*24) + pickup_hour - current_window) == 1):
        count_next_window_records += 1
        next_window_records.append(message['DOLocationID'])

    #check if pickup is in current window
    if drop_hour == current_window % 24:
        current_window_records.append(message['DOLocationID'])

    #check if pickup is in next window
    if ((((drop_day-1)*24) + drop_hour - current_window) == 1):
        next_window_records.append(message['DOLocationID'])

    #publish batched data
    if(len(current_window_records) > 3000):
        msg = {'window': current_window,
               'zones': current_window_records, 'time': current_window_time}
        publish(msg)
        current_window_records = []

    #if 500 records are from the next window
    #grace time for out of order records
    if (len(next_window_records) > 500):
        msg = {'window': current_window,
               'zones': current_window_records, 'time': current_window_time}
        publish(msg)
        current_window_records = next_window_records
        next_window_records = []
        count_next_window_records = 0
        current_window += 1
        current_window_time = message['tpep_pickup_datetime'][0:13]+':00:00'


def main():
    global EXIT
    #make necessary subscriptions
    subscription = Subscriber()
    subscription.subscribe('/queue/source', 'batch',
                           Listener(subscription, action))
    while not EXIT:
        pass
    subscription.disconnect()


if __name__ == '__main__':
    main()
