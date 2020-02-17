import json
from listener import Listener
from subscriber import Subscriber

def action(message):
    #print in required format
    print(message)

def main():
    hour_subscription=Subscriber()
    day_subscription=Subscriber()
    acc_subscription=Subscriber()
    #final report after join
    #subscriber.subscribe('/queue/report/hour', 6, Listener(subscriber,action))
    hour_subscription.subscribe('/queue/report/hour', 'report_hour', Listener(hour_subscription,action))
    day_subscription.subscribe('/topic/report/day', 'report_day', Listener(day_subscription,action))
    acc_subscription.subscribe('/queue/report/day_accident', 'accident_day', Listener(acc_subscription,action))
    while not True:
        pass

if __name__ == '__main__':
    main()