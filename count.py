
from collections import Counter
import json
from listener import Listener
from publisher import Publisher
from subscriber import Subscriber
publisher = Publisher()
EXIT = False

current_window = 0
current_window_time = '2018-01-01 00:00:00'
freq = dict()

#action to be performed on message
def action(message):
    global current_window
    global freq
    #publish pending messages on exit
    if message == 'exit':
        msg = {'zones': freq, 'window': current_window,
               'time': message['time']}
        publisher.publish(json.dumps(msg), '/queue/count')
        publisher.disconnect()
        EXIT = True
        return

    window = message['window']
    
    #if next window starts publish data
    if(window == current_window):
        freq = Counter(message['zones'])+Counter(freq)
    else:
        msg = {'zones': freq, 'window': window, 'time': message['time']}
        publisher.publish(json.dumps(msg), '/queue/count')
        current_window_time = message['time']
        current_window = window
        freq = Counter(message['zones'])


def main():
    global EXIT
    subscription = Subscriber()
    subscription.subscribe('/queue/batch', 'count',
                           Listener(subscription, action))
    while not EXIT:
        pass
    subscription.disconnect()


if __name__ == '__main__':
    main()
