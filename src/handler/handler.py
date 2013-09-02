#-*- coding: utf8 -*-

import logging
import json
import memcache


log = logging.getLogger(__name__)


class Handler(object):
    """
    main process handler
    """

    def __init__(self):
        self._mc = memcache.Client(['192.168.1.13:11211'])

    def pre_process(self, body):
        """ pre process
        user config and tlv2json input
        """
        try:
            self._recv = json.loads(body)
            log.info(" [pre_process] :%s",
                     json.dumps(self._recv, sort_keys=True, indent=4))
            return True
        except Exception as e:
            log.error("Unexpected error:%r", e)
            self._build_empty_return_params()
            return False

    def do_process(self):
        try:
            log.info(" [do_process]")
            self._build_return_params(locals())
        except Exception as e:
            log.error("Unexpected error:%r", e)
            self._build_empty_return_params()
            return

    def _build_return_params(self, kargs):
        params = {}
        #params["user"] = kargs["user"]
        self._ret_msg = json.dumps(params)

    def _build_empty_return_params(self):
        params = {}
        params["action"] = []
        self._ret_msg = json.dumps(params)
