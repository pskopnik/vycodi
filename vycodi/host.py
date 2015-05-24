from vycodi.bucket import FileBucket, JSONFileBucket
from vycodi.httpserver import Server
from vycodi.daemon import Daemon
from vycodi.utils import redisFromConfig, ensureJSONData, storeJSONData
from os.path import join, abspath, exists
from os import mkdir
from io import IOBase
import json

def fromConfig(redis, config, *args, **kwargs):
	if redis is None:
		redis = redisFromConfig(config)
	address = (config['address'], int(config['port']))
	runDir = abspath(config['runDir'])
	if not exists(runDir):
		mkdir(runDir)

	pidFile = join(runDir, 'daemon.pid')
	logFile = join(runDir, 'daemon.log')
	bucketFile = join(runDir, 'bucket.json')
	ensureJSONData(bucketFile, [])

	hostId = None
	try:
		with open(join(runDir, 'data.json')) as f:
			data = json.load(f)
			hostId = data['hostId']
	except FileNotFoundError:
		pass

	obj = HostDaemon(address, redis, pidFile, *args, id=hostId, bucket=bucketFile, logFile=logFile, **kwargs)

	if hostId is None:
		storeJSONData(join(runDir, 'data.json'), {'hostId': obj._host.id})
	return obj


class HostDaemon(Daemon):
	def __init__(self, address, redis, *args, id=None, bucket=None, logFile=None, **kwargs):
		super(HostDaemon, self).__init__(*args, **kwargs)
		self._host = Host(address, redis, id=id, bucket=bucket)
		self._logFile = logFile

	def _run(self, *args, **kwargs):
		self._host.start()
		self.wait()

	def _shutdown(self):
		self._host.shutdown()
		self._host.join()


class Host(object):
	"""Host for files
	"""
	def __init__(self, address, redis, id=None, bucket=None):
		"""Init
		address must be a two element tuple address = (bindAddress, bindPort)
		If id is not set (is None), the next available host id is fetched
		bucket may be a FileBucket object, file or file path (which is then loaded),
		or an IOBase instance
		"""
		self._redis = redis
		self._address = address
		self._server = None
		if id is None:
			self.id = self._fetchNextId()
		else:
			self.id = id
		if bucket is None:
			self.bucket = FileBucket(redis, self)
		elif isinstance(bucket, str):
			self.bucket = JSONFileBucket(redis, self, bucket)
		elif isinstance(bucket, IOBase):
			self.bucket = FileBucket(redis, self)
			self.bucket.loadJSON(bucket)
		else:
			bucket.host = self
			self.bucket = bucket

	def start(self):
		if self._server is None:
			self._server = Server(self._address, self.bucket)
		self._server.start()
		self._register()

	def shutdown(self):
		self._unregister()
		self._server.shutdown()
		try:
			self.bucket.store()
		except Exception:
			pass

	def join(self):
		if self._server is not None:
			self._server.join()

	def _register(self):
		self._redis.hmset('vycodi:host:' + str(self.id), {
			'address': self._address[0],
			'port': self._address[1]
		})
		self._redis.sadd('vycodi:hosts', self.id)
		self.bucket.register()

	def _unregister(self):
		self.bucket.unregister()
		self._redis.srem('vycodi:hosts', self.id)
		self._redis.delete('vycodi:host:' + str(self.id))

	def _fetchNextId(self):
		return self._redis.incr('vycodi:hosts:index')
