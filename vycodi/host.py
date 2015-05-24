from vycodi.bucket import FileBucket
from vycodi.httpserver import Server

class HostDaemon(object):
	def __init__(self, address, redis, id=None, bucket=None):
		self._host = Host(address, redis, id=id, bucket=bucket)

	def run(self, *args, **kwargs):
		def handler(*args):
			self._host.shutdown()
			self._host.join()

		signal.signal(signal.SIGTERM, handler)
		self._host.start()
		try:
			signal.pause()
		except KeyboardInterrupt:
			handler()

class Host(object):
	"""Host for files
	"""
	def __init__(self, address, redis, id=None, bucket=None):
		"""Init
		address must be a two element tuple address = (bindAddress, bindPort)
		"""
		self._redis = redis
		self._address = address
		if id is None:
			self.id = self._fetchNextId()
		else:
			self.id = id
		self._server = None
		if bucket is None:
			self.bucket = FileBucket(redis, self)
		elif isinstance(bucket, str):
			self.bucket = FileBucket(redis, self)
		else:
			self.bucket = bucket

	def start(self):
		if self._server is None:
			self._server = Server(self._address, self.bucket)
		self._server.start()
		self._register()

	def shutdown(self):
		self._unregister()
		self._server.shutdown()

	def join(self):
		if self._server is not None:
			self._server.join()

	def _register(self):
		self._redis.hmset('vycodi:host:' + self.id, {
			'address': self._address[0],
			'port': self._address[1]
		})
		self._redis.sadd('vycodi:hosts', self.id)

	def _unregister(self):
		self._redis.srem('vycodi:hosts', self.id)
		self._redis.delete('vycodi:host:' + self.id)

	def _fetchNextId(self):
		return self._redis.incr('vycodi:hosts:index')
