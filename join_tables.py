import json
import pandas as pd

from listener import Listener
from publisher import Publisher
from subscriber import Subscriber

from constants import MVC_PATH
from constants import LOOKUP_PATH

publisher=Publisher()


def hourly_action(msg):
    ##publish the hourly results
    publisher.publish(json.dumps(msg), '/queue/report/hour')
    if 

def daily_action(msg):
    ##publish the daily results
    publisher.publish(json.dumps(msg), '/queue/report/day')
    pass

def main():
    ##read the crash and lookup csv
    mvc_df = pd.read_csv('MVC_PATH')
    lookup_df = pd.read_csv('LOOKUP_PATH')
    


    joined_data={
    'tpep_pickup_datetime':message['tpep_pickup_datetime'],
    'tpep_dropoff_datetime':message['tpep_dropoff_datetime'],
    'PULocationID':message['PULocationID'],
    'DOLocationID':message['DOLocationID']
    }
    
    ##join with the tables
    hour_subscription=Subscriber()
    hour_subscription.subscribe('/queue/analytics/hour', 3, Listener(hour_subscription,hourly_action))
    day_subscription=Subscriber()
    day_subscription.subscribe('/topic/analytics/day', 3, Listener(day_subscription,daily_action))

if __name__ == '__main__':
    main()