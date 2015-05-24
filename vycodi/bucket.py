from vycodi.utils import loadJSONData, storeJSONData

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
	def type(self):
		return self._type

	@type.setter
	def type(self, type):
		self._type = type
		if self.bucket is not None:
			self.bucket.updateFile(self, ['type'])

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
	key_base = 'vycodi:file:'

	def __init__(self, redis, host):
		self._files = dict()
		self._writeLocks = dict()
		self._redis = redis
		self._registered = False
		self.host = host

	def __getitem__(self, key):
		if isinstance(key, File):
			key = key.id
		return self._files[key]

	def __delitem__(self, key):
		if isinstance(key, File):
			key = key.id
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
			l = self._redis.lock(self.key_base + str(f.id) + ':lock', timeout=0.5, sleep=0.1)
			l.acquire()
			self._redis.hmset(self.key_base + str(f.id), f.exportRedis())
			self._redis.sadd(self.key_base + str(f.id) + ":hosts", self.host.id)
			l.release()
		self._registered = True

	def _registerFile(self, file):
		# TODO LOCK
		l = self._redis.lock(self.key_base + str(file.id) + ':lock', timeout=0.5, sleep=0.1)
		l.acquire()
		self._redis.hmset(self.key_base + str(file.id), file.exportRedis())
		self._redis.sadd(self.key_base + str(file.id) + ":hosts", self.host.id)
		l.release()

	def unregister(self):
		for f in self._files.values():
			# TODO LOCK
			if f.id in self._writeLocks:
				self._writeLocks[f.id].release()
			l = self._redis.lock(self.key_base + str(f.id) + ':lock', timeout=0.5, sleep=0.1)
			l.acquire()
			self._redis.srem(self.key_base + str(f.id) + ":hosts", self.host.id)
			if self._redis.scard(self.key_base + str(f.id) + ":hosts") < 1:
				self._redis.delete(self.key_base + str(f.id))
				self._redis.delete(self.key_base + str(f.id) + ':hosts')
			l.release()
		self._registered = False

	def _unregisterFile(self, file):
		# TODO LOCK
		if f.id in self._writeLocks:
			self._writeLocks[f.id].release()
		l = self._redis.lock(self.key_base + str(file.id) + ':lock', timeout=0.5, sleep=0.1)
		l.acquire()
		self._redis.srem(self.key_base + str(file.id) + ":hosts", self.host.id)
		if self._redis.scard(self.key_base + str(file.id) + ":hosts") < 1:
			self._redis.delete(self.key_base + str(file.id))
			self._redis.delete(self.key_base + str(file.id) + ':hosts')
		l.release()

	def _fetchNextId(self):
		return self._redis.incr('vycodi:files:index')

	def updateFile(self, file, *args):
		fileExp = file.exportRedis()
		if len(args) == 0:
			data = fileExp
		else:
			data = dict()
			for arg in args:
				data = fileExp[arg]
		self._redis.hmset(self.key_base + file.id, data)

	def writeLockFile(self, file):
		if file.id in self._writeLocks:
			return
		# TODO LOCK
		l = self._redis.lock(self.key_base + file.id)
		l.acquire()
		self._writeLocks[file.id] = l

	def releaseWriteLockFile(self, file):
		if file.id not in self._writeLocks:
			return
		self._writeLocks[file.id].release()
		del self._writeLocks[file.id]

	def loadJSON(self, f, register=False):
		data = loadJSONData(f)
		for fDict in data:
			self._files[fDict['id']] = File(
				fDict['id'], fDict['name'],
				fDict['path'], fDict['type']
			)
			if register:
				l = self._redis.lock(self.key_base + f.id + ':lock', timeout=0.5, sleep=1)
				self._redis.hmset(self.key_base + fDict['id'], fDict)
				self._redis.sadd(self.key_base + fDict['id'] + ":hosts", self.host.id)
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
		self.loadJSON(self._path)

	def store(self):
		self.exportJSON(self._path)

	# def __del__(self):
	# 	self.store()

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
