#!/usr/bin/env python3

import os
from vycodi.utils import loadJSONConfig, storeJSONConfig

with open('/etc/hosts', 'r') as f:
	line = f.readline()
	ip = line.split()[0]

config = loadJSONConfig('docker/host_config.json')
config['address'] = ip
if "REDIS_PORT_6379_TCP_ADDR" in os.environ:
	config['dbhost'] = os.environ["REDIS_PORT_6379_TCP_ADDR"]
if "REDIS_PORT_6379_TCP_PORT" in os.environ:
	config['dbport'] = os.environ["REDIS_PORT_6379_TCP_PORT"]
storeJSONConfig('docker/host_config.json', config)

