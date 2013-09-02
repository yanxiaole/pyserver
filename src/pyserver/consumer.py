#-*- coding: utf8 -*-

import sys
import pika
import logging
import time

from producer import Producer
from handler.handler import Handler

log = logging.getLogger(__name__)


class Consumer(object):
    """
    A consumer implementation that consumes specified
    exchange and routing_keys
    """

    def _on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        log.info("connection established")
        #self._add_on_connection_close_callback()
        self._open_channel()

    def _close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        log.info('Closing the channel')
        self._channel.close()

    def _open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        try:
            log.info("connection channel created")
            self._channel = channel
            self._setup_exchange(self.exchange)

            log.info("exchange %s declared as %s" % (self.exchange, 'topic'))
        except Exception as e:
            log.error("Unexpected error:%r", e)
            raise e

    def _setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        try:
            log.info('Declaring exchange %s', exchange_name)
            self._channel.exchange_declare(callback=self._on_exchange_declareok,
                                           exchange=self.exchange,
                                           type='topic',
                                           durable=True,
                                           auto_delete=False,)
        except Exception as e:
            log.error("Unexpected error:%r", e)
            raise e

    def _on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        log.info('Exchange declared')
        self._setup_queue(self.queue_name)

    def _setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        try:
            log.info('Declaring queue %s', queue_name)
            # make queue and message durability
            self._channel.queue_declare(queue=self.queue_name, durable=True,
                                        exclusive=False, auto_delete=False,
                                        callback=self._on_queue_declaredok)
        except Exception as e:
            log.error("Unexpected error:%r", e)
            raise e

    def _on_queue_declaredok(self, method_frame):
        """Called when RabbitMQ has told us our Queue has been declared,
            frame is the response from RabbitMQ"""
        try:
            # bingding exchange and queue
            for routing_key in self.routing_keys:
                log.info("binding queue %s to exchange %s by routing_key %s" %
                         (self.queue_name, self.exchange, routing_key))
                self._channel.queue_bind(exchange=self.exchange,
                                         queue=self.queue_name,
                                         routing_key=routing_key,
                                         callback=self._on_bindok)
        except Exception as e:
            log.error("Unexpected error:%r", e)
            raise e

    def _on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        log.info('Queue bound')
        self._start_consuming()

    def _start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        try:
            log.info('Issuing consumer related RPC commands')
            self._add_on_cancel_callback()
            #self.channel.basic_qos(prefetch_count=1)
            self._consumer_tag = self._channel.basic_consume(self.consume_msg_callback,
                                                             queue=self.queue_name)
        except Exception as e:
            log.error("Unexpected error:%r", e)
            raise e

    def _add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        log.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        log.info('Consumer was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self._channel.close()

    def _connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        parameters = pika.ConnectionParameters(host=self.host, heartbeat_interval=30)
        while True:
            try:
                log.info('Connecting to %s', self.host)
                return pika.SelectConnection(parameters, self._on_connection_open, stop_ioloop_on_close=False)
            except Exception:
                log.warning('%s cannot connect', self.host, exc_info=True)
                time.sleep(10)
                continue

    def __init__(self, host, consumer_exchange, consumer_routing_keys,
                 consumer_queue_name, producer_exchange, producer_routing_key):
        try:
            self.handler = Handler()

            log.info("init consumer for host=%s, exchange=%s, routing_keys=%s" %
                     (host, consumer_exchange, repr(consumer_routing_keys)))

            self.host = host
            self.exchange = consumer_exchange
            self.routing_keys = consumer_routing_keys
            self.queue_name = consumer_queue_name

            if not consumer_routing_keys:
                log.error("routing_keys can't be null...")
                sys.exit(1)

            # init producer
            self.resend_producer = Producer(self.host,
                                            producer_exchange,
                                            producer_routing_key
                                            )

            # init pika connection
            self._connection = self._connect()
            self._closing = False
            self._consumer_tag = None

        except Exception as e:
            log.error("Unexpected error:%r", e)

    def start_consuming(self):
        """ main consuming loop """
        #self.channel.start_consuming()
        # Loop so we can communicate with RabbitMQ
        self._connection.ioloop.start()

    def _on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        log.info('RabbitMQ acknowledged the cancellation of the consumer')
        self._close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            log.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._on_cancelok, self._consumer_tag)

    def consume_msg_callback(self, ch, method, properties, body):
        """
        send msg to jpush, if failed, push back to MQ for retry
        """
        try:
            """
            *** IMPLEMENT the handler 'pre_process' and 'do_process' func ***
            """
            log.info(" [x] %r:%r", method.routing_key, body,)

            if self.handler.pre_process(body):
                if self.handler.do_process():
                    self.__reply(self.handler._ret_msg)

        except Exception as e:
            log.error("Unexpected error:%r", e)
        finally:
            # MUST have this ACK statement, otherwise the mem will be filled up by
            # RabbitMQ's unack queue. USE
            # sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged
            # to check this may came bug
            log.info('Acknowledging message %s', method.delivery_tag)
            self._channel.basic_ack(delivery_tag=method.delivery_tag)
            #ch.basic_ack(delivery_tag=method.delivery_tag)

    def __reply(self, message):
        """ reply the message """
        log.info("reply:%s", message)
        self.responser.produce_send_msg(message)

    def close(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        log.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        log.info('Stopped')
