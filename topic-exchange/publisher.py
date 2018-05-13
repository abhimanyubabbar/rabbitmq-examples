import json
import logging
import os

import pika
from pika.spec import (
    BasicProperties,
    Exchange,
    Queue
)

logging.basicConfig(level=logging.INFO)


def main():

    url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)

    channel = connection.channel()

    # Declare the exchange for the type audit
    resp = channel.exchange_declare(
        exchange='audit',
        exchange_type='topic',
        durable=True,
        auto_delete=True
    )

    assert type(resp.method) is Exchange.DeclareOk, "{}".format(type(resp.method))
    logging.info("Successfully created the exchange")

    resp = channel.queue_declare(
        queue='user-audit',
        durable=True,
        auto_delete=True
    )
    assert type(resp.method) is Queue.DeclareOk, "{}".format(type(resp.method))

    resp = channel.queue_bind(
        queue='user-audit',
        exchange='audit',
        routing_key='audit.records.user.#',
    )
    assert type(resp.method) is Queue.BindOk, "{}".format(type(resp.method))

    pub_resp = channel.basic_publish(
        exchange='audit',
        routing_key='audit.records.user.deletions',
        body=json.dumps({'type': 'user.deletions1', 'body': {'id': 123, 'location': 'XYZ'}}),
        properties=BasicProperties(delivery_mode=2)  # durable message
    )
    assert pub_resp is True
    logging.info("Successfully published the message to the broker")


if __name__ == '__main__':
    main()
