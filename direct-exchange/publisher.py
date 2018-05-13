import json
import logging
import os

import pika

logging.basicConfig(level=logging.INFO)

url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
params = pika.URLParameters(url)
params.socket_timeout = 5

connection = pika.BlockingConnection(parameters=params)
channel = connection.channel()
channel.queue_declare(queue='pdfprocess')


bdy = json.dumps({'state': 'something'})

channel.basic_publish(
    exchange='',
    routing_key='pdfprocess',
    body=bdy
)

logging.info("Message Published successfully")
channel.close()
