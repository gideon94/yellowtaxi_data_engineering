import json
import pandas as pd
import datetime

from listener import Listener
from publisher import Publisher
from subscriber import Subscriber

from constants import MVC_PATH
from constants import LOOKUP_PATH

publisher=Publisher()
lookup_df = pd.read_csv('LOOKUP_PATH')
mvc_df = pd.read_csv('MVC_PATH')

def hourly_action(msg):
    ##publish the hourly results
    global lookup_df
    filter_list = msg['peak_zones']
    filter_lookup = lookup_df[lookup_df['LocationID'].isin(filter_list)]
    filter_lookup = filter_lookup[['Borough','Zone']]

    mvc_df['DATE'] = pd.to_datetime(mvc_df['CRASH DATE'] + ' ' + mvc_df['CRASH TIME'])
    time = datetime.datetime.strptime(time, '%d-%m-%Y %H:%M:%S')
    time_ = time + datetime.timedelta(hours = 1)
    from_time = '{:%Y-%m-%d %H:00:00}'.format(time)
    to_time = '{:%Y-%m-%d %H:00:00}'.format(time_)

    filter_mvc = mvc_df.loc[(mvc_df['DATE'] > from_time) & (mvc_df['DATE'] < to_time) & mvc_df['BOROUGH'].isin(filter_lookup['Borough'])]
    filter_mvc = filter_mvc.dropna(subset=['BOROUGH'])
    filter_mvc = filter_mvc[['BOROUGH','LOCATION','CONTRIBUTING FACTOR VEHICLE 1']]

    freq_report={'zones':filter_lookup.to_json(orient='records'), 'freq':msg['trips']},
    publisher.publish(freq_report, '/queue/report/hour')

    accident_report={'accident report':filter_mvc.to_json(orient = 'records')},
    publisher.publish(msg,'/queue/report/hour')

def daily_action(msg):
    ##publish the daily results
<<<<<<< HEAD
=======
    publisher.publish(json.dumps(msg), '/queue/report/day_accident')
    pass
>>>>>>> b9bd69b78870f31c287b6bec5a0182a064f60305

    mvc_df['DATE'] = pd.to_datetime(mvc_df['CRASH DATE'] + ' ' + mvc_df['CRASH TIME'])
    time = datetime.datetime.strptime(time, '%d-%m-%Y %H:%M:%S')
    time_ = time + datetime.timedelta(hours = 1)
    from_time = '{:%Y-%m-%d %H:00:00}'.format(time)
    to_time = '{:%Y-%m-%d %H:00:00}'.format(time_)

    filter_mvc = mvc_df.loc[(mvc_df['DATE'] > from_time) & (mvc_df['DATE'] < to_time)]
    filter_mvc = filter_mvc.dropna(subset=['BOROUGH'])
    filter_mvc = filter_mvc[['BOROUGH','LOCATION','CONTRIBUTING FACTOR VEHICLE 1']]

    accident_report={'accident report':filter_mvc.to_json(orient = 'records')},
    publisher.publish(msg,'/queue/report/day')


def main():
    ##read the crash and lookup csv
    
    ##join with the tables
    hour_subscription=Subscriber()
    hour_subscription.subscribe('/queue/enrich/hour', 3, Listener(hour_subscription,hourly_action))
    day_subscription=Subscriber()
    day_subscription.subscribe('/topic/analytics/day', 3, Listener(day_subscription,daily_action))

if __name__ == '__main__':
    main()