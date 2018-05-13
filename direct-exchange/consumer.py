import logging
import os
import time

import pika

logging.basicConfig(level=logging.INFO)


def pdf_process_function(msg):

    time.sleep(10)
    logging.info("Pdf processing finished")
    logging.info(msg)
    return


def callback(ch, method, properties, body):
    pdf_process_function(body)


# Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()  # start a channel
channel.queue_declare(queue='pdfprocess')  # Declare a queue

channel.basic_consume(
    consumer_callback=callback,
    queue='pdfprocess',
    no_ack=True
)

# start consuming is a blocking function.
channel.start_consuming()
connection.close()
