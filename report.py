import datetime
from collections import defaultdict
import json
from listener import Listener
import math
from publisher import Publisher
from subscriber import Subscriber

#hourly freq for each day
freqs_hourly = {}
publisher = Publisher()
EXIT = False


def publish_daily_report(freqs_hourly):
    global publisher
    global EXIT
    #calculate peak time
    max_trips = 0
    peak_times = []
    for time, freq in freqs_hourly.items():
        check_max = max(max_trips, freq)
        if(freq == max_trips):
            peak_times.append(time)
        elif(check_max > max_trips):
            max_trips = check_max
            peak_times = [time]

    msg = {'peak_times': peak_times, 'trips': max_trips}
    publisher.publish(json.dumps(msg), '/topic/report/day')
    if EXIT:
        publisher.publish('exit', '/topic/enrich/hour')
        publisher.publish('exit', '/topic/report/day')
        publisher.disconnect()


def publish_hourly_report(peak_zones, trips, time):
    global publisher
    msg = {'peak_zones': peak_zones, 'trips': trips, 'time': time}
    #publish to join data with crash table
    publisher.publish(json.dumps(msg), '/topic/enrich/hour')


def action(message):
    global EXIT
    global freqs_hourly
    #current window frequency
    freq_current = 0
    #max freq for current window by zone
    max_freq = 0
    #busiest zone in current window
    max_zones = []

    if(message == 'exit'):
        EXIT = True
        publish_daily_report(freqs_hourly)
        return

    current_window = int(message['window'])
    current_time = message['time']

    for zone, freq in message['zones'].items():
        check_freq = max([freq, max_freq])
        if(freq == max_freq):
            max_zones.append(zone)
        elif(check_freq > max_freq):
            max_freq = check_freq
            max_zones = [zone]
        freq_current += freq

    publish_hourly_report(max_zones, max_freq, message['time'])
    #add current windows frequency to daily report
    freqs_hourly[current_time] = freq_current

    #publish report at the end of the day
    if(current_window != 0 and current_window % 24 == 0):
        publish_daily_report(freqs_hourly)
        freqs_hourly = {}


def main():
    global EXIT
    subscription = Subscriber()
    subscription.subscribe('/queue/count', 'report_',
                           Listener(subscription, action))
    while not EXIT:
        pass
    subscription.disconnect()


if __name__ == '__main__':
    main()
