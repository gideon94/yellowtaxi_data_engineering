from connection import create_connection
import stomp

class Subscriber:
    conn=None
    exit=False
    def subscribe(self, destination, id, listener):
        self.conn= create_connection()
        self.conn.set_listener('clean', listener)
        self.conn.subscribe(destination=destination, id=id, ack='auto')
        while not self.exit:
            pass
    
    def disconnect(self):
        self.exit=True
        self.conn.disconnect()