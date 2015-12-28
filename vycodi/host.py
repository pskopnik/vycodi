from vycodi.bucket import FileBucket, FileSystemFile, JSONFileBucket, validFileTypes
from vycodi.httpserver import Server as HTTPServer
from vycodi.daemon import Daemon
from vycodi.utils import redisFromConfig, ensureJSONData, storeJSONData, loadJSONData
from vycodi.jsonrpc import RPCClient, Server as RPCServer, Dispatcher, JSONRPCDispatchException
from vycodi.heartbeat import Heartbeat, Purger
from os.path import join, abspath, exists, dirname
from os import mkdir, access, R_OK, W_OK
from io import IOBase
from threading import Thread
import logging


class HostDaemon(Daemon):
	def __init__(self, host, *args, **kwargs):
		super(HostDaemon, self).__init__(*args, **kwargs)
		self.host = host

	def run(self, *args, **kwargs):
		self.host.start()
		self.wait()

	def shutdown(self):
		self.host.shutdown()
		self.host.join()

	@classmethod
	def fromConfig(cls, config, *args, redis=None, **kwargs):
		runDir = abspath(config['runDir'])
		if not exists(runDir):
			mkdir(runDir)
		host = Host.fromConfig(config, redis=redis)
		return cls(host, *args, runDir=runDir, **kwargs)


class Host(Purger):
	"""Host for files
	"""
	def __init__(self, address, redis, id=None, bucket=None, rpcAddress=None):
		"""Init
		address must be a two element tuple address = (bindAddress, bindPort)
		If id is not set (is None), the next available host id is fetched
		bucket may be a FileBucket object, file or file path (which is then loaded),
		or an IOBase instance
		"""
		self._redis = redis
		self._address = address
		self._server = None
		self._rpcAddress = rpcAddress
		self._rpcServer = None
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		self.id = id if id is not None else self._fetchNextId()
		if bucket is None:
			self.bucket = FileBucket(redis, self)
		elif isinstance(bucket, str) or isinstance(bucket, IOBase):
			self.bucket = JSONFileBucket(redis, self, bucket)
		else:
			bucket.host = self
			self.bucket = bucket
		self.heartbeat = Heartbeat(
			redis, str(self.id),
			60,
			30,
			prefix="vycodi:host:",
			setKey="vycodi:hosts",
			purger=self
		)

	def start(self):
		self._logger.info("Starting...")
		if self._server is None:
			self._server = HTTPServer(self._address, self.bucket)
		if self._rpcAddress is not None:
			if self._rpcServer is None:
				self._rpcServer = HostRPCServer(self._rpcAddress, self)
			self._rpcServer.start()
		self._server.start()
		self._register()
		self.heartbeat.start()
		self.bucket.load()

	def shutdown(self):
		self._logger.info("Shutting down...")
		self._unregister()
		self.heartbeat.signalStopIntent()
		self._server.shutdown()
		self._rpcServer.shutdown()
		self.bucket.store()

	def _purge(self, prefix, key, postfix, heartbeat):
		# remove from files
		pass

	def zombie(self, prefix, key, postfix, heartbeat):
		self._logger.warn("Became zombie, restarting")
		self.shutdown()
		self.start()

	def join(self):
		if self._server is not None:
			self._server.join()
		if self._rpcServer is not None:
			self._rpcServer.join()
		self.heartbeat.join()

	def rpcClient(self):
		return HostRPCClient(self._rpcAddress)

	@classmethod
	def fromConfig(cls, config, redis=None):
		if redis is None:
			redis = redisFromConfig(config)
		address = (config['address'], int(config['port']))
		runDir = abspath(config['runDir'])
		if not exists(runDir):
			mkdir(runDir)

		rpcSock = join(runDir, 'rpc.sock')
		bucketFile = join(runDir, 'bucket.json')
		ensureJSONData(bucketFile, [])

		backendConf = config.get('backend', {})
		backendType = backendConf.get('backend', 'filesystem')
		if backendType == 'filesystem':
			from vycodi.bucket import FileSystemBackend
			backend = FileSystemBackend.fromBackendConfig(backendConf)
		elif backendType == 's3':
			from vycodi.s3backend import S3Backend
			backend = S3Backend.fromBackendConfig(backendConf)
		else:
			raise ValueError("Unknown backendType '{}'".format(backendType))

		bucket = JSONFileBucket(redis, None, bucketFile, backend=backend)

		hostId = None
		try:
			hostId = loadJSONData(join(runDir, 'data.json'))['hostId']
		except FileNotFoundError:
			pass

		host = cls(address, redis, id=hostId, bucket=bucket, rpcAddress=rpcSock)

		if hostId is None:
			storeJSONData(join(runDir, 'data.json'), {'hostId': host.id})
		return host

	def _register(self):
		self._logger.info("Registering...")
		self._redis.hmset('vycodi:host:' + str(self.id), {
			'id': self.id,
			'address': self._address[0],
			'port': self._address[1]
		})
		self._redis.sadd('vycodi:hosts', self.id)
		self.bucket.register()

	def _unregister(self):
		self._logger.info("Unregistering...")
		self.bucket.unregister()
		self._redis.srem('vycodi:hosts', self.id)
		self._redis.delete('vycodi:host:' + str(self.id))

	def _fetchNextId(self):
		return self._redis.incr('vycodi:hosts:index')


class HostRPCServer(Thread):
	def __init__(self, address, host):
		super(HostRPCServer, self).__init__()
		self._host = host
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		dispatcher = Dispatcher()
		dispatcher.add_method(self.genAddFile())
		self._server = RPCServer(address, dispatcher)

	def run(self):
		self._logger.info("Starting...")
		self._server.serve_forever()

	def genAddFile(self):
		def addFile(name, path, type):
			if type not in validFileTypes:
				raise JSONRPCDispatchException(
					code=101,
					message="Invalid type supplied"
				)
			mode = W_OK if type in ('w',) else R_OK
			if not (access(path, mode) or (mode == W_OK and access(dirname(path), mode))):
				raise JSONRPCDispatchException(
					code=111,
					message="Path not accessible"
				)
			f = FileSystemFile(None, name, path, type)
			self._host.bucket.add(f)
			self._logger.info("Added file %s - %s @ %s", f.id, f.name, f.path)
			return f.id
		return addFile

	def shutdown(self):
		self._logger.info("Shutting down...")
		self._server.shutdown()


class HostRPCClient(RPCClient):
	def addFile(self, name, path, type):
		with self.sock() as sock:
			reqId = self._sendRequest(sock, 'addFile', [name, path, type])
			response = self._recvResponse(sock, reqId)
			return response['result']

	@classmethod
	def fromConfig(cls, config):
		runDir = abspath(config['runDir'])
		if not exists(runDir):
			return None

		return cls(join(runDir, 'rpc.sock'))
