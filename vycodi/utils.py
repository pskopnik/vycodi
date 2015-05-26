import json
from redis import StrictRedis
from io import IOBase
from os.path import exists

def loadJSONConfig(file):
	if isinstance(file, IOBase):
		return json.load(file)
	else:
		with open(file, 'r') as f:
			return json.load(f)

def storeJSONConfig(file, data):
	if isinstance(file, IOBase):
		json.dump(data, file, indent='\t')
	else:
		with open(file, 'w') as f:
			json.dump(data, f, indent='\t')

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
		return json.loads(d[name])
	except (KeyError, ValueError):
		return default

def storeJSONField(d, name, data):
	d[name] = json.dumps(data, separators=(',', ':'))

def ensureJSONData(filePath, default):
	if not exists(filePath):
		storeJSONData(filePath, default)

def redisFromConfig(config):
	host = config.get('dbhost', 'localhost')
	port = int(config.get('dbport', 6379))
	db = int(config.get('dbdb', 0))
	password = config.get('dbpassword', None)
	return StrictRedis(host=host, port=port, db=db, password=password)

def decodeRedis(d, encoding='utf-8', errors='strict'):
	n = dict()
	for k in d:
		n[k.decode(encoding=encoding, errors=errors)] = d[k].decode(
			encoding=encoding, errors=errors
		)
	return n
