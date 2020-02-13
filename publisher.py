from connection import create_connection

class Publisher:
    conn=None

    def __init__(self):
        self.conn=create_connection()

    def publish(self, msg, destination):
        self.conn.send(body=msg, destination=destination)
    
    def disconnect(self):
        self.conn.disconnect()

    

    
