import json
from listener import Listener
from publisher import Publisher
import stomp
from subscriber import Subscriber
import time

publisher=Publisher()
def publish_clean_data(message):
    global publisher
    publisher.publish(json.dumps(message), '/queue/preprocess/cleanup')


def action(message):
    cleaned={
    'tpep_pickup_datetime':message['tpep_pickup_datetime'],
    'tpep_dropoff_datetime':message['tpep_dropoff_datetime'],
    'PULocationID':message['PULocationID'],
    'DOLocationID':message['DOLocationID']
    }
    publish_clean_data(cleaned)

def main():
    subscription=Subscriber()
    subscription.subscribe('/queue/source', 1, Listener(subscription,action))

if __name__ == '__main__':
    main()