#!/usr/bin/env python3

import os
from vycodi.utils import loadJSONConfig, storeJSONConfig

config = loadJSONConfig('host_config.json')
if "REDIS_PORT_6379_TCP_ADDR" in os.environ:
	config['dbhost'] = os.environ["REDIS_PORT_6379_TCP_ADDR"]
if "REDIS_PORT_6379_TCP_PORT" in os.environ:
	config['dbport'] = os.environ["REDIS_PORT_6379_TCP_PORT"]
storeJSONConfig('host_config.json', config)

