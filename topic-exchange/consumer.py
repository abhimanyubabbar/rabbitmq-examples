import json
import logging
import os
import time

import pika
from pika.spec import (
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

    channel.basic_consume(
        consumer_callback=cb,
        queue='user-audit',
        no_ack=False
    )

    logging.info("Starting with consuming the messages")
    channel.start_consuming()


def cb(ch, method, properties, body):

    payload = json.loads(body)
    try:

        tp = payload['type']

        if tp == "user.signups":
            logging.info("user Signups ingestion beginning")
            time.sleep(10)

        elif tp == "user.deletions":
            logging.info("User deletion ingestion beginning")
            time.sleep(5)

        else:
            raise Exception("Unknown type")

        logging.info("Finished working, acking the message")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception:
        logging.error("Rejecting / Dropping unknown message")
        ch.basic_reject(method.delivery_tag, requeue=False)


if __name__ == "__main__":
    main()
