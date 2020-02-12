#!/usr/bin/env python
from connection import connection
from constants import TRIPDATA_PATH 
import csv
import json
from publisher import publish
import stomp


def main():

    numbers = list(range(0, 100))
    
    with open( TRIPDATA_PATH, "r" ) as zone_data:
        reader = csv.DictReader(zone_data)
        for line in reader:
            publish(connection, json.dumps(line), '/source')

    publish(connection, str('exit'), '/source')

    connection.disconnect()


if __name__ == '__main__':
    main()
