#!/usr/bin/env python
from constants import TRIPDATA_PATH 
import csv
import json
from publisher import Publisher
import stomp


def main():
    publisher = Publisher()
    with open( TRIPDATA_PATH, "r" ) as zone_data:
        reader = csv.DictReader(zone_data)
        for line in reader:
            publisher.publish( json.dumps(line), '/queue/source')

    publisher.publish( str('exit'), '/source')

    publisher.disconnect()


if __name__ == '__main__':
    main()
