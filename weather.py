import json
import pandas as pd
import requests
from constants import LOOKUP_PATH
from listener import Listener
from publisher import Publisher
from subscriber import Subscriber

lookup_df = pd.read_csv(LOOKUP_PATH)
publisher = Publisher()
EXIT = False


def action(msg):
    if(msg == 'exit'):
        EXIT = True
        publisher.disconnect()
    filter_list = msg['peak_zones']
    filter_lookup = lookup_df[lookup_df['LocationID'].isin(filter_list)]
    filter_lookup = filter_lookup[['Borough']]
    borough = filter_lookup.values.tolist()[0][0]
    if borough == 'EWR' or borough == 'Unknown':
        borough = 'New York'
    #use the borough in the lookup to get weather data
    #gives the current weather of the city
    resp = requests.get('http://api.openweathermap.org/data/2.5/weather?q=' +
                        borough+'&appid=f2572bf74819ef294fa56da03fbb0b7d')
    if resp.status_code != 200:
        publisher.publish('Data Not Available', '/queue/report/weather')
    else:
        publisher.publish(json.dumps(resp.json()), '/queue/report/weather')


def main():
    subscription = Subscriber()
    subscription.subscribe('/topic/enrich/hour', 'weather',
                           Listener(subscription, action))
    while not EXIT:
        pass


if __name__ == '__main__':
    main()
