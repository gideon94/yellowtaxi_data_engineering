
from listener import Listener
import stomp
from subscriber import Subscriber
import time

def action(message):
    print('message:')
    print(message)

def main():
    subscription=Subscriber()
    subscription.subscribe('/source', 1, Listener(subscription,action))

if __name__ == '__main__':
    main()