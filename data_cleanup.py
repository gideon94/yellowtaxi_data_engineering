
from listener import Listener
import stomp
from subscriber import Subscriber
import time

subscription=None
class Cleanup_Listener(Listener):
    def on_message(self, headers, message):
        print('message:')
        print(message)
        if 'exit' in message:
            subscription.disconnect()


def main():
    global subscription
    subscription=Subscriber()
    subscription.subscribe('/source',1,Cleanup_Listener())

if __name__ == '__main__':
    main()