import pika, sys


class Publisher:
    def __init__(self, host, port) -> None:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.connection.channel()

    def publish(self, exchange, exchange_type, routing_key, message):
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

    def close_connection(self):
        self.connection.close()


routing_key = sys.argv[1] if len(sys.argv) > 1 else ""
pub = Publisher("127.0.0.1", 5672)
# pub.publish("logs", "fanout", routing_key, "Hello, world")
pub.publish("logs_by_topic", "topic", routing_key, "Hello, world")
pub.close_connection()
