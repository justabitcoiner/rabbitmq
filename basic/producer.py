import pika


class Producer:
    def __init__(self, host, port) -> None:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.connection.channel()

    def send_message(self, exchange, queue, message):
        # durable=True option make this queue survive a RabbitMQ node restart.
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=queue,
            body=message,
            # This option make this message survive a RabbitMQ node restart.
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )

    def close_connection(self):
        self.connection.close()


p = Producer("127.0.0.1", 5672)
p.send_message("", "basic", "Hello, world")
p.close_connection()
