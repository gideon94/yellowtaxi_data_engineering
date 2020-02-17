import json
import pandas as pd
import datetime

from listener import Listener
from publisher import Publisher
from subscriber import Subscriber

from constants import MVC_PATH
from constants import LOOKUP_PATH

publisher=Publisher()
lookup_df = pd.read_csv(LOOKUP_PATH)
mvc_df = pd.read_csv(MVC_PATH)
mvc_df['DATE'] = pd.to_datetime(mvc_df['CRASH DATE'] + ' ' + mvc_df['CRASH TIME'])

print('started')
def hourly_action(msg):
    ##publish the hourly results
    print('received')
    print(msg)
    global lookup_df
    global publisher
    filter_list = msg['peak_zones']
    filter_lookup = lookup_df[lookup_df['LocationID'].isin(filter_list)]
    filter_lookup = filter_lookup[['Borough','Zone']]

    freq_report={'peak_zones':filter_lookup.to_json(orient='records'), 'freq':msg['trips']}
    print(freq_report)
    publisher.publish(json.dumps(freq_report), '/queue/report/hour')

    time = datetime.datetime.strptime(time, '%d-%m-%Y %H:%M:%S')
    time_ = time + datetime.timedelta(hours = 1)
    from_time = '{:%Y-%m-%d %H:00:00}'.format(time)
    to_time = '{:%Y-%m-%d %H:00:00}'.format(time_)

    filter_mvc = mvc_df.loc[(mvc_df['DATE'] > from_time) & (mvc_df['DATE'] < to_time) & mvc_df['BOROUGH'].isin(filter_lookup['Borough'])]
    filter_mvc = filter_mvc.dropna(subset=['BOROUGH'])
    filter_mvc = filter_mvc[['BOROUGH','LOCATION','CONTRIBUTING FACTOR VEHICLE 1']]

    accident_report={'accident_report':filter_mvc.to_json(orient = 'records')}
    print(accident_report)
    publisher.publish(json.dumps(accident_report), '/queue/report/hour')

def daily_action(msg):
    print('received')
    print(msg)
    global publisher
    for time in msg['peak_times']:
        ##publish the daily results
        time = datetime.datetime.strptime(time, '%d-%m-%Y %H:%M:%S')
        time_ = time + datetime.timedelta(hours = 1)
        from_time = '{:%Y-%m-%d %H:00:00}'.format(time)
        to_time = '{:%Y-%m-%d %H:00:00}'.format(time_)

        filter_mvc = mvc_df.loc[(mvc_df['DATE'] > from_time) & (mvc_df['DATE'] < to_time)]
        filter_mvc = filter_mvc.dropna(subset=['BOROUGH'])
        filter_mvc = filter_mvc[['BOROUGH','LOCATION','CONTRIBUTING FACTOR VEHICLE 1']]

        accident_report={'accident report':filter_mvc.to_json(orient = 'records')}
        print(accident_report)
        publisher.publish(json.dumps(accident_report),'/queue/report/day_accident')

def main():
    ##read the crash and lookup csv
    
    ##join with the tables
    hour_subscription=Subscriber()
    hour_subscription.subscribe('/queue/enrich/hour', 3, Listener(hour_subscription,hourly_action))
    day_subscription=Subscriber()
    day_subscription.subscribe('/topic/report/day', 3, Listener(day_subscription,daily_action))

if __name__ == '__main__':
    main()