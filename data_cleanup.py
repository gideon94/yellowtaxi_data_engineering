import json
from listener import Listener
from publisher import Publisher
import stomp
from subscriber import Subscriber
import time

publisher=Publisher()
EXIT=False

def publish_clean_data(message):
    global publisher
    publisher.publish(json.dumps(message), '/queue/preprocess/cleanup')


def action(message):
    global EXIT

    if message=='exit':
        publish_clean_data('exit')
        publisher.disconnect()
        EXIT=True
        return
        
    cleaned={
    'tpep_pickup_datetime':message['tpep_pickup_datetime'],
    'tpep_dropoff_datetime':message['tpep_dropoff_datetime'],
    'PULocationID':message['PULocationID'],
    'DOLocationID':message['DOLocationID']
    }
    publish_clean_data(cleaned)


def main():
    global EXIT
    subscription=Subscriber()
    subscription.subscribe('/queue/source', 'clean', Listener(subscription,action))
    while not EXIT:
        pass
    subscription.disconnect()

if __name__ == '__main__':
    main()