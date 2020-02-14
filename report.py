import datetime
from collections import defaultdict
import json
from .publisher import Publisher

def action(message):
    current_day=0
    if(current_day!=message['day']):
        pass #clear data
    freq_hourly=0
    zone_hourly=[{'zone':0}]
    aggregate_time=defaultdict(int)

    aggregate_zone_daily=defaultdict(int)
    aggregate_time[message['time']]=0

    max_zones_hour=[]
    max_freq_zone=0
    for zone in message['zones'].keys():
        aggregate_zone_daily[zone] += message['zones'][zone]
        aggregate_time[message['time']] += message['zones'][zone]
        max_check=max([max_freq_zone,message['zones'][zone]])

        if(max_freq_zone==max_check):
            max_zones_hour.append(zone)
        else:
            max_zones_hour=[zone]
    publisher=Publisher()
    publisher.publish(json.dumps(message), '/queue/report')
    

def main():
    #subscribe to enrich data
    pass

if __name__ == '__main__':
    main()