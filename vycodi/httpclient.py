import requests
from vycodi.utils import decodeRedis
from os.path import abspath

class FileLoaderException(Exception):
	pass


class FileNotFound(FileLoaderException):
	def __init__(self, id):
		super(FileNotFound, self).__init__("FileNotFound, id = " + str(id))
		self.id = id


class FileNotAvailable(FileLoaderException):
	def __init__(self, id):
		super(FileNotAvailable, self).__init__("FileNotAvailable, id = " + str(id))
		self.id = id


class HostNotAvailable(FileLoaderException):
	def __init__(self, id):
		super(HostNotAvailable, self).__init__("HostNotAvailable, id = " + str(id))
		self.id = id


class LoaderNotSet(FileLoaderException):
	def __init__(self):
		super(LoaderNotSet, self).__init__("LoaderNotSet")


class PathNotSet(FileLoaderException):
	def __init__(self):
		super(PathNotSet, self).__init__("PathNotSet")


class File(object):
	def __init__(self, id, name, type, path=None, loader=None):
		self.id = id
		self.name = name
		self.type = type
		self.path = path
		self.loader = loader

	def download(self, path=None):
		if self.loader is None:
			raise LoaderNotSet()
		if path is not None:
			self.path = path
		self.loader.download(self.id, self.path)

	def upload(self, path=None):
		if self.loader is None:
			raise LoaderNotSet()
		if path is not None:
			self.path = path
		self.loader.upload(self.id, self.path)

	def open(self, *args, **kwargs):
		if path is None:
			raise PathNotSet()
		return open(self.path, *args, **kwargs)


class FileLoader(object):
	def __init__(self, redis, pool=None):
		self._redis = redis
		if pool is None:
			pool = ClientPool()
		self._pool = pool

	def download(self, id, outF):
		f = self.getFile(id, fObj=outF)
		s = self._pool[self._getServerAddress(id)]
		s.download(id, outF)
		return f

	def upload(self, id, inF):
		f = self.getFile(id, fObj=inF)
		s = self._pool[self._getServerAddress(id)]
		s.upload(id, inF)
		return f

	def getFile(self, id, fObj=None):
		"""Gets file data from redis server
		id is file id, must be str(.) compatible
		If fObj is set, it tries to deduct its abspath and
		sets it as the path of the File
		Also sets the loader of the file to this FileLoader
		"""
		fDict = decodeRedis(self._redis.hgetall('vycodi:file:' + str(id)))
		if len(fDict) == 0:
			raise FileNotFound(id)
		f = File(int(fDict['id']), fDict['name'], fDict['type'], loader=self)
		if fObj is not None:
			if isinstance(fObj, str):
				f.path = abspath(fObj)
			else:
				try:
					f.path = abspath(fObj.name)
				except AttributeError:
					pass
		return f

	def _getServerAddress(self, id):
		hostId = self._redis.srandmember('vycodi:file:' + str(id) + ':hosts')
		if hostId is None:
			raise FileNotAvailable(id)
		hostId = hostId.decode('utf-8')
		hostDict = self._redis.hgetall('vycodi:host:' + hostId)
		return (hostDict[b'address'].decode('utf-8'), int(hostDict[b'port']))


class HTTPClientException(Exception):
	def __init__(self, retCode, message):
		super(HTTPClientException, self).__init__()
		self.retCode = retCode
		self.message = message


class ClientPool(object):
	def __init__(self):
		self._clients = dict()

	def __getitem__(self, key):
		if isinstance(key, tuple):
			key = key[0] + ':' + str(key[1])
		if key not in self._clients:
			self.add(Client(key))
		return self._clients[key]

	def add(self, client):
		serverStrAdr = client.serverStrAdr
		if serverStrAdr in self._clients:
			del self._clients[serverStrAdr]
		self._clients[serverStrAdr] = client


class Client(object):
	def __init__(self, server):
		if isinstance(server, tuple):
			self.serverAdr = server
			self.serverStrAdr = server[0] + ':' + str(server[1])
		else:
			self.serverStrAdr = server
			if ":" not in server:
				self.serverAdr = (server, 80)
			else:
				adrList = server.split(":")
				self.serverAdr = (adrList[0], int(adrList[1]))
		self._s = requests.Session()
		self.baseUrl = 'http://' + self.serverStrAdr + '/'

	def download(self, id, outF):
		r = self._s.get(self.baseUrl + str(id), stream=True)
		if not r.status_code == requests.codes.ok:
			raise HTTPClientException(r.status_code, r.text)
		if isinstance(outF, str):
			with open(outF, 'wb') as outFO:
				for chunk in r.iter_content(chunk_size=1024 * 1024):
					if chunk:
						outFO.write(chunk)
				outFO.flush()
		else:
			for chunk in r.iter_content(chunk_size=1024 * 1024):
				if chunk:
					outF.write(chunk)
			outF.flush()

	def upload(self, id, inF):
		if isinstance(inF, str):
			with open(inF, 'rb') as inFO:
				r = self._s.post(self.baseUrl + str(id), data=inFO)
		else:
			r = self._s.post(self.baseUrl + str(id), data=inF)
		if not r.status_code == requests.codes.ok:
			raise HTTPClientException(r.status_code, r.text)
