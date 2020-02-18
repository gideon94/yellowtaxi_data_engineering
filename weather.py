import json
import requests
from listener import Listener
from publisher import Publisher
from subscriber import Subscriber
publisher=Publisher()
EXIT=False

def action(msg):
    if(msg=='exit'):
        EXIT=True
        publisher.disconnect()
    resp = requests.get('http://api.openweathermap.org/data/2.5/weather?q=New York&appid=f2572bf74819ef294fa56da03fbb0b7d')
    if resp.status_code != 200:
        publisher.publish('Data Not Available','/queue/report/weather')
    else:
        publisher.publish(json.dumps(resp.json()),'/queue/report/weather')

def main():
    subscription=Subscriber()
    subscription.subscribe('/topic/enrich/hour', 'weather', Listener(subscription,action))
    while not EXIT:
        pass

if __name__ == '__main__':
    main()