import json
from .listener import Listener
from .subscriber import Subscriber

def action(message):
    #print in required format
    print(message)
    pass

def main():
    subscriber=Subscriber()
    #final report after join
    subscriber.subscribe('/queue/report/day', 4, Listener(subscriber,action))
    subscriber.subscribe('/queue/report/hour', 5, Listener(subscriber,action))
