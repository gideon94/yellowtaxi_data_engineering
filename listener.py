
import json
import stomp

class Listener(stomp.ConnectionListener):

    def __init__(self,subscriber,action):
        self.subscriber=subscriber
        self.action=action

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        if 'exit' in message:
            self.subscriber.disconnect()
        self.action(json.loads(message))
