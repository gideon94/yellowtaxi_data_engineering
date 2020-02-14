import json
from .listener import Listener
from .publisher import Publisher
from .subscriber import Subscriber

publisher=Publisher()

def publish_report(msg):
    publisher.publish(json.dumps(msg), '/queue/report/hour')

def action():
    ##read the crash and location csv
    ##join with the tables
    ##publish the results

    pass

def main():
    subscription=Subscriber()
    subscription.subscribe('/queue/analytics/hour_report', 3, Listener(subscription,action))

if __name__ == '__main__':
    main()