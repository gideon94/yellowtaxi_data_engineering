import time
import stomp
from sub import Listener

def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.connect(login='system', passcode='manager', wait=True)

if __name__ == '__main__':
    main()