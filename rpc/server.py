import pika


class Server:
    def __init__(self, host, port) -> None:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.connection.channel()

    def start(self, queue):
        self.channel.queue_declare(queue=queue)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue, on_message_callback=self.on_request)

        print("[*] Awaiting RPC requests")
        self.channel.start_consuming()

    def on_request(self, channel, method, props, body):
        print(f"[x] Processing message. Body: {body}")
        n = int(body)
        response = do_something(n)
        channel.basic_publish(
            exchange="",
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=str(response),
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)


def do_something(n):
    # Find fibbonaci
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return do_something(n - 1) + do_something(n - 2)


server = Server("127.0.0.1", 5672)
server.start("rpc")
