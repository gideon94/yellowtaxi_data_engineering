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
lookup_df['Borough'] = lookup_df['Borough'].str.upper()
lookup_df['Zone'] = lookup_df['Zone'].str.upper()
mvc_df = pd.read_csv(MVC_PATH)
dates = ['01/01/2018','01/02/2018','01/03/2018','01/04/2018','01/05/2018','01/06/2018','01/07/2018','01/08/2018',
'01/09/2018','01/10/2018','01/11/2018','01/12/2018','01/13/2018','01/14/2018','01/15/2018','01/16/2018','01/17/2018',
'01/18/2018','01/19/2018','01/20/2018','01/21/2018','01/22/2018','01/23/2018','01/24/2018','01/25/2018','01/26/2018',
'01/27/2018','01/28/2018','01/29/2018','01/30/2018','01/31/2018']
mvc_df=mvc_df.loc[mvc_df['CRASH DATE'].isin(dates)]
mvc_df['DATE'] = pd.to_datetime(mvc_df['CRASH DATE'] + ' ' + mvc_df['CRASH TIME'])
EXIT=False

def hourly_action(msg):
    ##publish the hourly results
    global lookup_df
    global publisher
    if msg=='exit':
        publisher.publish('exit', '/queue/report/hour')
        publisher.publish('exit', '/queue/report/hour_accident')
        return
    filter_list = msg['peak_zones']
    filter_lookup = lookup_df[lookup_df['LocationID'].isin(filter_list)]
    filter_lookup = filter_lookup[['Borough','Zone']]
    time = datetime.datetime.strptime(msg['time'], '%Y-%m-%d %H:%M:%S')
    time_ = time + datetime.timedelta(hours = 1)
    from_time = '{:%Y-%m-%d %H:00:00}'.format(time)
    to_time = '{:%Y-%m-%d %H:00:00}'.format(time_)

    freq_report={'peak_zones':filter_lookup.to_dict('records'),
                'time' :from_time+' - ' + to_time,
                'freq':msg['trips']}
    publisher.publish(json.dumps(freq_report), '/queue/report/hour')

    filter_mvc = mvc_df.loc[(mvc_df['DATE'] > from_time) & (mvc_df['DATE'] < to_time) & mvc_df['BOROUGH'].isin(filter_lookup['Borough'])]
    filter_mvc = filter_mvc.dropna(subset=['BOROUGH'])
    filter_mvc = filter_mvc[['CRASH DATE','CRASH TIME','BOROUGH','CONTRIBUTING FACTOR VEHICLE 1']]


    accident_report={'accident_report': filter_mvc.to_dict('records')}
    publisher.publish(json.dumps(accident_report), '/queue/report/hour_accident')

def daily_action(msg):
    global EXIT
    global publisher
    if msg=='exit':
        EXIT=True
        publisher.publish('exit','/queue/report/day_accident')
        publisher.disconnect()
        return
    for time in msg['peak_times']:
        ##publish the daily results
        time = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        time_ = time + datetime.timedelta(hours = 1)
        from_time = '{:%Y-%m-%d %H:00:00}'.format(time)
        to_time = '{:%Y-%m-%d %H:00:00}'.format(time_)

        filter_mvc = mvc_df.loc[(mvc_df['DATE'] > from_time) & (mvc_df['DATE'] < to_time)]
        filter_mvc = filter_mvc.dropna(subset=['BOROUGH'])
        filter_mvc = filter_mvc[['CRASH DATE','CRASH TIME','BOROUGH','CONTRIBUTING FACTOR VEHICLE 1']]

        accident_report={'accident_report':filter_mvc.to_dict('records')}
        publisher.publish(json.dumps(accident_report),'/queue/report/day_accident')


def main():
    global EXIT
    
    ##join with the tables
    hour_subscription=Subscriber()
    hour_subscription.subscribe('/topic/enrich/hour', 'join_hour', Listener(hour_subscription,hourly_action))
    day_subscription=Subscriber()
    day_subscription.subscribe('/topic/report/day', 'join_day', Listener(day_subscription,daily_action))
    while not EXIT:
        pass
if __name__ == '__main__':
    main()