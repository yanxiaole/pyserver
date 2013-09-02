#-*- coding: utf8 -*-

import pika
import logging

log = logging.getLogger("Producer")


class Producer(object):
    """
    A producer implementation that produce the resend msg
    """

    def _set_binding(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host,
                                      #heartbeat_interval=30
                                      ))
        channel = connection.channel()
        channel.exchange_declare(exchange=self._exchange, type='topic',
                                 durable=True, auto_delete=False,)
        channel.queue_declare(queue="queue.push", durable=True,
                              exclusive=False, auto_delete=False)
        channel.queue_bind(exchange=self._exchange, queue="queue.push",
                           routing_key=self._routing_key)

    def __init__(self, host, exchange, routing_key):
        self._exchange = exchange
        self._routing_key = routing_key
        self._host = host

        log.info("init producer for host=%s, exchange=%s, routing_key=%s"
                 % (host, exchange, routing_key))

        # just for set up exchange, routing_key and queue_name at first time
        self._set_binding()

    def produce_send_msg(self, message):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host,
                                      #heartbeat_interval=30
                                      ))
        self._channel = self._connection.channel()

        self._channel.basic_publish(exchange=self._exchange,
                                    routing_key=self._routing_key,
                                    body=message,
                                    properties=pika.BasicProperties(
                                        # make message persistent
                                        delivery_mode=2,
                                    ))
        log.info(" [x] %r:%r" % (self._routing_key, message))
        self._connection.close()
