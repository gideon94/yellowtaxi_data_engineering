import json
from .listener import Listener
from .publisher import Publisher
from .subscriber import Subscriber

publisher=Publisher()


def hourly_action(msg):
    ##publish the hourly results
    publisher.publish(json.dumps(msg), '/queue/report/hour')
    pass

def daily_action(msg):
    ##publish the daily results
    publisher.publish(json.dumps(msg), '/queue/report/day')
    pass

def main():
    ##read the crash and location csv
    ##join with the tables
    hour_subscription=Subscriber()
    hour_subscription.subscribe('/queue/analytics/hour', 3, Listener(hour_subscription,hourly_action))
    day_subscription=Subscriber()
    day_subscription.subscribe('/topic/analytics/hour', 3, Listener(day_subscription,daily_action))

if __name__ == '__main__':
    main()