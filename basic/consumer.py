import pika, time


class Consumer:
    def __init__(self, host, port) -> None:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.connection.channel()

    def on_receive(self, channel, method, props, body):
        body = body.decode()
        print(f"Receive message: {body}")
        time.sleep(3)
        print(f"Done process message: {body}")
        # Signal RabbitMQ server that this message has been processed successfully, so server can delete this message.
        self.channel.basic_ack(method.delivery_tag)

    def start(self, queue):
        # durable=True option make this queue survive a RabbitMQ node restart.
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)  # Fair dispatch
        self.channel.basic_consume(queue=queue, on_message_callback=self.on_receive)
        print("[*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()


c = Consumer("127.0.0.1", 5672)
c.start("basic")
