
from collections import Counter
from listener import Listener
from publisher import Publisher
from subscriber import Subscriber
publisher=Publisher()
EXIT=False

current_window=0
current_window_time='2018-01-01 00:00:00'

def action(message):
    global current_window
    freq=dict()
    window=message['window']

    if(window==current_window):
        freq=Counter(message['zones'])+Counter(freq)
    else:
        publisher.publish('/queue/analytics/hour')
        current_window_time=message['time']
        current_window=window
    

def main():
    global EXIT

    while not EXIT:
        pass
    pass



if __name__ == '__main__':
    main()