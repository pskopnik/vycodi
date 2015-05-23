import argh
import json
from vycodi.host import Host
from redis import StrictRedis

def loadJSONFile(filePath):
	with open(filePath, 'r') as f:
		return json.load(f)

def loadJSONFile(filePath):
	with open(filePath, 'w') as f:
		return json.dump(f, indent='\t')

def redisFromConfig(config):
	host = config.get('dbhost', 'localhost')
	port = int(config.get('dbport', 6379))
	db = int(config.get('dbdb', 0))
	password = config.get('dbpassword', None)
	return StrictRedis(host=host, port=port, db=db, password=password)

def startHost(configFile):
	config = loadJSONFile(configFile)
	redis = redisFromConfig(config)
	bindAddress = config['address']
	bindPort = int(config['port'])

	hostObj = Host((bindAddress, bindPort), redis)

parser = argh.ArghParser()
parser.add_commands((startHost,), namespace="host")

def main():
	parser.dispatch()

if __name__ == '__main__':
	main()
