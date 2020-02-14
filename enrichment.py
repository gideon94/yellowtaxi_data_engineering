import csv
import json
import stomp
import time
from .constants import MVC_PATH
from .listener import Listener
from .publisher import Publisher
from .subscriber import Subscriber

def publish_clean_data(message):
    publisher=Publisher()
    publisher.publish(json.dumps(message), '/queue/preprocess/cleanup')
    print(message)


def action(message):
    cleaned={
    'tpep_pickup_datetime':message['tpep_pickup_datetime'],
    'tpep_dropoff_datetime':message['tpep_dropoff_datetime'],
    'PULocationID':message['PULocationID'],
    'DOLocationID':message['DOLocationID']
    }
    publish_clean_data(cleaned)

def main():
    with open( MVC_PATH, "r" ) as mvc_data:
        reader = csv.DictReader(mvc_data)
    subscription=Subscriber()
    subscription.subscribe('/queue/preprocess/cleanup', 1, Listener(subscription,action))

if __name__ == '__main__':
    main()