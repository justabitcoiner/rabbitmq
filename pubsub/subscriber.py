import pika, time, sys


class Subscriber:
    def __init__(self, host, port) -> None:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.connection.channel()

    def on_receive(self, channel, method, props, body):
        body = body.decode()
        print(f"Receive message: {body}")
        time.sleep(3)
        print(f"Done process message: {body}")

    def subscribe(self, exchange, exchange_type, routing_keys):
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        result = self.channel.queue_declare(queue="", exclusive=True)
        queue = result.method.queue

        if routing_keys:
            for key in routing_keys:
                self.channel.queue_bind(queue=queue, exchange=exchange, routing_key=key)
        else:
            self.channel.queue_bind(queue=queue, exchange=exchange)
        self.channel.basic_consume(queue=queue, on_message_callback=self.on_receive, auto_ack=True)

        print("[*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()


routing_keys = sys.argv[1:]
sub = Subscriber("127.0.0.1", 5672)
# sub.subscribe("logs", "fanout", routing_keys)
sub.subscribe("logs_by_topic", "topic", routing_keys)
