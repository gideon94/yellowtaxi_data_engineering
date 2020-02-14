import datetime
from collections import defaultdict
import json
from .listener import Listener
import math
from .publisher import Publisher
from .subscriber import Subscriber

#hourly freq for each day
freqs_hourly={}
publisher=Publisher()

def publish_daily_report(freqs_hourly, current_window):

    #calculate peak time
    max_trips=0
    peak_times=[]
    for window, freq in freqs_hourly.items():
        check_max = max(max_trips, freq)
        if(check_max == max_trips):
            peak_times.append(window)
        elif():
            peak_times=[window]
    peak_windows = [str(window-1) +' - '+ str(window%24) for window in peak_times]

    msg={'day': math.ceil(current_window/24), 'peak_times':peak_windows}
    publisher.publish(json.dumps(msg), '/queue/report/day')

def publish_hourly_report(peak_zones, trips):
    msg={'peak_zones':peak_zones,'trips':trips}
    #publish to join data with crash table
    publisher.publish(json.dumps(msg), '/queue/analytics/hour_report')

def action(message):
    global freqs_hourly

    #current window frequency
    freq_current=0
    #max freq for current window by zone
    max_freq=0
    #busiest zone in current window
    max_zones=[]

    current_window=message['window']

    for zone, freq in message['zones'].items():
        check_freq=max([freq,max_freq])
        if(check_freq == max_freq):
            max_zones.append(zone)
        elif(check_freq>max_freq):
            max_zones=[zone]
        freq_current+=freq
    
    publish_hourly_report(max_zones,max_freq)
    #add cuurent windows frequency to daily report
    freqs_hourly['current_window']=freq_current
    
    #publish report at the end of the day
    if(current_window!=0 & current_window%24==0):
        publish_daily_report(freqs_hourly,current_window)
        freqs_hourly={}

def main():
    subscription=Subscriber()
    subscription.subscribe('/queue/analytics/hour_window', 2, Listener(subscription,action))

if __name__ == '__main__':
    main()