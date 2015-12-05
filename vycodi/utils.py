import json
from io import IOBase
from os.path import exists
import six


def dumpJSON(data):
	return json.dumps(data, separators=(',', ':'))


def loadJSON(string):
	if isinstance(string, six.binary_type):
		string = string.decode('utf-8')
	return json.loads(string)


def loadJSONConfig(file):
	if isinstance(file, IOBase):
		return json.load(file)
	else:
		with open(file, 'r') as f:
			return json.load(f)


def storeJSONConfig(file, data):
	indent = '\t'
	if six.PY2:
		indent = 4
	if isinstance(file, IOBase):
		json.dump(data, file, indent=indent)
	else:
		with open(file, 'w') as f:
			json.dump(data, f, indent=indent)


def loadJSONData(file):
	if isinstance(file, IOBase):
		return json.load(file)
	else:
		with open(file, 'r') as f:
			return json.load(f)


def storeJSONData(file, data):
	if isinstance(file, IOBase):
		json.dump(data, file, separators=(',', ':'))
	else:
		with open(file, 'w') as f:
			json.dump(data, f, separators=(',', ':'))


def loadJSONField(d, name, default=None):
	try:
		return loadJSON(d[name])
	except (KeyError | ValueError):
		return default


def storeJSONField(d, name, data):
	d[name] = dumpJSON(data)


def ensureJSONData(filePath, default):
	if not exists(filePath):
		storeJSONData(filePath, default)


def redisFromConfig(config):
	global StrictRedis
	host = config.get('dbhost', 'localhost')
	port = int(config.get('dbport', 6379))
	db = int(config.get('dbdb', 0))
	password = config.get('dbpassword', None)
	try:
		return StrictRedis(host=host, port=port, db=db, password=password)
	except NameError:
		from redis import StrictRedis
		return StrictRedis(host=host, port=port, db=db, password=password)


def decodeRedis(d, encoding='utf-8', errors='strict'):
	if isinstance(d, dict):
		n = dict()
		for k in d:
			n[k.decode(encoding=encoding, errors=errors)] = d[k].decode(
				encoding=encoding, errors=errors
			)
		return n
	elif isinstance(d, list):
		n = []
		for v in d:
			n.append(v.decode(encoding=encoding, errors=errors))
		return n
	elif isinstance(d, six.binary_type):
		return d.decode(encoding=encoding, errors=errors)
	else:
		return d
