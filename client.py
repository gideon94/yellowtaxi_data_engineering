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
    subscriber.subscribe('/queue/report', 1, Listener(subscriber,action))
