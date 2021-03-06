from constants import ACTIVEMQ_PORT
import stomp

def create_connection():
    conn = stomp.Connection(host_and_ports=[('localhost', ACTIVEMQ_PORT)])
    conn.connect(login='system', passcode='manager', wait=True)
    return conn
