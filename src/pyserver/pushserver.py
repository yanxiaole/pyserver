#!/usr/bin/python
#-*- coding: utf8 -*-

import sys
import logging
import logging.handlers

sys.path.append('../')

from daemonize import Daemonize
from consumer import Consumer

pidfile = "/tmp/pushserver.pid"
RLL_FILENAME = '../../log/rlog_push.out'
ERR_FILENAME = '../../log/error_push.out'

LOG_FORMAT = ('%(asctime)s|%(levelname)s|%(name)s|%(lineno)d|%(message)s')


def consume_example():
    consumer = Consumer(
            host="192.168.1.13",
            #host="10.0.0.13",
            consumer_exchange="exchange.push.in",
            consumer_routing_keys=["topic.push.in"],
            consumer_queue_name="queue.push",
            producer_exchange="exchange.push.out",
            producer_routing_key="topic.push.out")
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        # Someone pressed CTRL-C, stop consuming and close
        consumer.close()
    except Exception as e:
        consumer.close()
        raise e


def main():
    logging.basicConfig(level=logging.INFO,
                        format=LOG_FORMAT,
                        )
    # create rotating file handler which logs even info messages
    rllfh = logging.handlers.RotatingFileHandler(
                    RLL_FILENAME, maxBytes=10*1024*1024, backupCount=5)
    rllfh.setLevel(logging.INFO)

    # create error log file handler which logs error messages
    errfh = logging.FileHandler(ERR_FILENAME)
    errfh.setLevel(logging.ERROR)

    # create formatter and add it to the handlers
    rllformatter = logging.Formatter(LOG_FORMAT)
    errformatter = logging.Formatter(LOG_FORMAT)
    rllfh.setFormatter(rllformatter)
    errfh.setFormatter(errformatter)

    # add the handlers to the logger
    logging.getLogger('').handlers = []
    logging.getLogger('').addHandler(rllfh)
    logging.getLogger('').addHandler(errfh)

    consume_example()


if __name__ == "__main__":
    daemon = Daemonize(app="PushServer", pidfile=pidfile, action=main,)
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
    """

    main()
    """
