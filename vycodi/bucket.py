from vycodi.utils import loadJSONData, storeJSONData
import logging

validFileTypes = ('r', 'w', 'l')

class File(object):
	"""File known to the system
	id is the id of the file in the database
	name is the name of the file, typically the last path component
	path is the absolute path to the file on the local system
	type is 'r' (readable, i.e. servable file), 'w' (writable, to be
		uploaded), 'l' (locked, readable after upload)
	"""
	def __init__(self, id, name, path, type, bucket=None):
		self.id = id
		self.name = name
		self.path = path
		self._type = type
		self.bucket = bucket

	@property
	def id(self):
		return self._id

	@id.setter
	def id(self, id):
		if id is None:
			self._id = None
		else:
			self._id = int(id)

	@property
	def type(self):
		return self._type

	@type.setter
	def type(self, type):
		self._type = type
		if self.bucket is not None:
			self.bucket.updateFile(self, 'type')

	def readable(self):
		return self.type == 'r' or self.type == 'l'

	def writable(self):
		return self.type == 'w'

	def writeLock(self):
		self.bucket.writeLockFile(self)

	def export(self):
		return {
			"id": self.id,
			"name": self.name,
			"path": self.path,
			"type": self._type
		}

	def exportRedis(self):
		return {
			"id": self.id,
			"name": self.name,
			"type": self._type
		}

class FileBucket(object):
	"""Bucket of File objects, accessible by id
	Persistence by dumping all files as a json file
	Also registers files in the redis database
	"""
	keyBase = 'vycodi:file:'

	def __init__(self, redis, host):
		self._files = dict()
		self._writeLocks = dict()
		self._redis = redis
		self._registered = False
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		self.host = host

	def __getitem__(self, key):
		if isinstance(key, File):
			key = key.id
		elif not isinstance(key, int):
			key = int(key)
		return self._files[key]

	def __delitem__(self, key):
		if isinstance(key, File):
			key = key.id
		elif not isinstance(key, int):
			key = int(key)
		file = self._files[key]
		self._unregisterFile(file)
		file.bucket = None
		del self._files[key]

	def add(self, file):
		if file.id is None:
			file.id = self._fetchNextId()
		if file.id in self._files:
			del self[file.id]
		self._files[file.id] = file
		file.bucket = self
		if self._registered:
			self._registerFile(file)

	def register(self):
		for f in self._files.values():
			# TODO LOCK
			l = self._redis.lock(self.keyBase + str(f.id) + ':lock', timeout=0.5, sleep=0.1)
			l.acquire()
			self._redis.hmset(self.keyBase + str(f.id), f.exportRedis())
			self._redis.sadd(self.keyBase + str(f.id) + ":hosts", self.host.id)
			l.release()
		self._registered = True

	def _registerFile(self, file):
		# TODO LOCK
		l = self._redis.lock(self.keyBase + str(file.id) + ':lock', timeout=0.5, sleep=0.1)
		l.acquire()
		self._redis.hmset(self.keyBase + str(file.id), file.exportRedis())
		self._redis.sadd(self.keyBase + str(file.id) + ":hosts", self.host.id)
		l.release()

	def unregister(self):
		for f in self._files.values():
			# TODO LOCK
			if f.id in self._writeLocks:
				self._writeLocks[f.id].release()
			l = self._redis.lock(self.keyBase + str(f.id) + ':lock', timeout=0.5, sleep=0.1)
			l.acquire()
			self._redis.srem(self.keyBase + str(f.id) + ":hosts", self.host.id)
			if self._redis.scard(self.keyBase + str(f.id) + ":hosts") < 1:
				self._redis.delete(self.keyBase + str(f.id))
				self._redis.delete(self.keyBase + str(f.id) + ':hosts')
			l.release()
		self._registered = False

	def _unregisterFile(self, file):
		# TODO LOCK
		if f.id in self._writeLocks:
			self._writeLocks[f.id].release()
		l = self._redis.lock(self.keyBase + str(file.id) + ':lock', timeout=0.5, sleep=0.1)
		l.acquire()
		self._redis.srem(self.keyBase + str(file.id) + ":hosts", self.host.id)
		if self._redis.scard(self.keyBase + str(file.id) + ":hosts") < 1:
			self._redis.delete(self.keyBase + str(file.id))
			self._redis.delete(self.keyBase + str(file.id) + ':hosts')
		l.release()

	def _fetchNextId(self):
		return int(self._redis.incr('vycodi:files:index'))

	def updateFile(self, file, *args):
		fileExp = file.exportRedis()
		if len(args) == 0:
			data = fileExp
		else:
			data = dict()
			for arg in args:
				data = fileExp[arg]
		self._redis.hmset(self.keyBase + file.id, data)

	def writeLockFile(self, file):
		if file.id in self._writeLocks:
			return
		# TODO LOCK
		l = self._redis.lock(self.keyBase + file.id + ':writelock')
		l.acquire()
		self._writeLocks[file.id] = l

	def releaseWriteLockFile(self, file):
		if file.id not in self._writeLocks:
			return
		self._writeLocks[file.id].release()
		del self._writeLocks[file.id]

	def store(self):
		pass

	def load(self):
		pass

	def loadJSON(self, f, register=False):
		data = loadJSONData(f)
		for fDict in data:
			self._files[fDict['id']] = File(
				fDict['id'], fDict['name'],
				fDict['path'], fDict['type'], self
			)
			if register:
				l = self._redis.lock(self.keyBase + str(fDict['id']) + ':lock', timeout=0.5, sleep=0.1)
				l.acquire()
				self._redis.hmset(self.keyBase + str(fDict['id']), fDict)
				self._redis.sadd(self.keyBase + str(fDict['id']) + ":hosts", self.host.id)
				l.release()

	def exportJSON(self, f):
		data = []
		for file in self._files.values():
			data.append(file.export())
		storeJSONData(f, data)

class JSONFileBucket(FileBucket):
	def __init__(self, redis, host, fPath):
		super(JSONFileBucket, self).__init__(redis, host)
		self._path = fPath

	def load(self):
		self._logger.info("Loading JSONFileBucket from '%s'...", self._path)
		self.loadJSON(self._path, register=self._registered)
		self._logger.info("Loaded %s files from '%s' into JSONFileBucket", len(self._files), self._path)

	def store(self):
		self._logger.info("Storing JSONFileBucket %s files to '%s'...", len(self._files), self._path)
		self.exportJSON(self._path)


# TODO
class JournaledFileBucket(JSONFileBucket):
	def __init__(self, redis, host, fPath, journalPath):
		super(JournaledFileBucket, self).__init__(redis, host, fPath)
		self._journalPath = journalPath
		self._journalFile = open(journalPath, 'rw')

	def store(self):
		super(JournaledFileBucket, self).store()
		self._journalFile.seek(0)
		self._journalFile.truncate()
		self._journalFile.flush()

	def __del__(self):
		super(JournaledFileBucket, self).__del__()
		try:
			self._journalFile.close()
		except Exception:
			pass
