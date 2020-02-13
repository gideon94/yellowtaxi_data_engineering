#!/usr/bin/env python
from constants import TRIPDATA_PATH 
import csv
import json
from publisher import Publisher
import stomp


def main():

    numbers = list(range(0, 100))
    publisher = Publisher()
    # with open( TRIPDATA_PATH, "r" ) as zone_data:
    #     reader = csv.DictReader(zone_data)
    #     for line in reader:
    #         publisher.publish( json.dumps(line), '/source')
    for i in range(10):
        publisher.publish( str(i), '/source')

    publisher.publish( str('exit'), '/source')

    publisher.disconnect()


if __name__ == '__main__':
    main()
