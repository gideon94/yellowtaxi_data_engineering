import json
from listener import Listener
from subscriber import Subscriber

def action(message):
    #print in required format
    print(message)

def main():
    subscriber=Subscriber()
    #final report after join
    subscriber.subscribe('/topic/report/day', 4, Listener(subscriber,action))
    subscriber.subscribe('/queue/report/day_accident', 5, Listener(subscriber,action))
    subscriber.subscribe('/queue/report/hour', 6, Listener(subscriber,action))

if __name__ == '__main__':
    main()