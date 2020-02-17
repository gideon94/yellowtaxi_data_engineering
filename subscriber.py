from connection import create_connection
import stomp

class Subscriber:
    def __init__(self):
        self.exit=False
        self.conn= create_connection()

    def subscribe(self, destination, name, listener):
        self.conn.set_listener(name, listener)
        self.conn.subscribe(destination=destination, id=name, ack='auto')

    def start(self):
        while not self.exit:
            pass
    
    def disconnect(self):
        self.exit=True
        self.conn.disconnect()