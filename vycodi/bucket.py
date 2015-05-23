import json

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
		self._host = host

	def __getitem__(self, key):
		if isinstance(key, File):
			key = key.id
		return self._files[key]

	def __delitem__(self, key):
		if isinstance(key, File):
			key = key.id
		self.unregisterFile(self._files[key])
		del self._files[key]

	def add(self, file):
		if file.id in self._files:
			del self[file.id]
		self._files[file.id] = file
		self.registerFile(file)

	def register(self):
		for f in self._files.values():
			# TODO LOCK
			l = self._redis.lock(self.key_base + f.id + ':lock', timeout=0.5, sleep=0.1)
			l.acquire()
			self._redis.hmset(self.key_base + f.id, f.export())
			self._redis.sadd(self.key_base + f.id + ":hosts", self._host.id)
			l.release()

	def registerFile(self, file):
		# TODO LOCK
		l = self._redis.lock(self.key_base + file.id + ':lock', timeout=0.5, sleep=0.1)
		l.acquire()
		self._redis.hmset(self.key_base + file.id, file.export())
		self._redis.sadd(self.key_base + file.id + ":hosts", self._host.id)
		l.release()

	def unregister(self):
		for f in self._files.values():
			# TODO LOCK
			if f.id in self._writeLocks:
				self._writeLocks[f.id].release()
			l = self._redis.lock(self.key_base + f.id + ':lock', timeout=0.5, sleep=0.1)
			l.acquire()
			self._redis.srem(self.key_base + f.id + ":hosts", self._host.id)
			if self._redis.scard(self.key_base + f.id + ":hosts") < 1:
				self._redis.delete(self.key_base + f.id)
				self._redis.delete(self.key_base + f.id + ':hosts')
			l.release()

	def unregisterFile(self, file):
		# TODO LOCK
		if f.id in self._writeLocks:
			self._writeLocks[f.id].release()
		l = self._redis.lock(self.key_base + file.id + ':lock', timeout=0.5, sleep=0.1)
		l.acquire()
		self._redis.srem(self.key_base + file.id + ":hosts", self._host.id)
		if self._redis.scard(self.key_base + file.id + ":hosts") < 1:
			self._redis.delete(self.key_base + file.id)
			self._redis.delete(self.key_base + file.id + ':hosts')
		l.release()

	def updateFile(self, file, *args):
		fileExp = file.export()
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

	def loadJSON(self, f, register=True):
		if isinstance(f, str):
			with open(f, 'r'):
				data = json.load(f)
		else:
			data = json.load(f)
		for fDict in data:
			self._files[fDict['id']] = File(
				fDict['id'], fDict['name'],
				fDict['path'], fDict['type']
			)
			if register:
				l = self._redis.lock(self.key_base + f.id + ':lock', timeout=0.5, sleep=1)
				self._redis.hmset(self.key_base + fDict['id'], fDict)
				self._redis.sadd(self.key_base + fDict['id'] + ":hosts", self._host.id)
				l.release()

	def exportJSON(self, f):
		data = []
		for file in self._files.values():
			data.append(file.export())
		if isinstance(f, str):
			with open(f, 'w'):
				json.dump(data, f, separators=(',', ':'))
		else:
			json.dump(data, f, separators=(',', ':'))
