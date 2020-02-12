import stomp

connection = stomp.Connection(host_and_ports=[('localhost', '61613')])
connection.connect(login='system', passcode='manager', wait=True)