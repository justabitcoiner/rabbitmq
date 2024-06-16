import pika
import uuid


class Client:
    def __init__(self, host, port) -> None:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True,
        )
        self.correlation_id = None
        self.response = None

    def call(self, routing_key, n):
        self.correlation_id = str(uuid.uuid4())
        self.response = None

        self.channel.basic_publish(
            exchange="",
            routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
            ),
            body=str(n),
        )
        while not self.response:
            self.connection.process_data_events()
        return int(self.response)

    def on_response(self, channel, method, props, body):
        if props.correlation_id == self.correlation_id:
            self.response = body


client = Client("127.0.0.1", 5672)
result = client.call("rpc", 40)
print("=> result:", result)
