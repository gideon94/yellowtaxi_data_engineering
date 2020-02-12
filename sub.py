#!/usr/bin/env python
import time
import stomp
from subscriber import Listener

EXIT = False

def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.set_listener('', Listener())
    conn.connect(login='system', passcode='manager', wait=True)
    conn.subscribe(destination='/source', id=1, ack='auto')
    while not EXIT:
        time.sleep(0.1)

    conn.disconnect()


if __name__ == '__main__':
    main()
