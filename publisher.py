def publish(conn, msg, destination):
    conn.send(body=msg, destination=destination)