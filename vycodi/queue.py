from vycodi.utils import decodeRedis
from vycodi.httpclient import File

class Queue(object):
	def __init__(self, id, redis):
			self.id = id
			self._redis = redis
			self._taskLoader = TaskLoader(redis)

	def reserveTask(self, worker, timeout=0):
		"""Fetches and reserves the next queue in the task for the
		passed in worker
		timeout resembles socket.socket.settimeout()
		"""
		if timeout is 0:
			taskId = self._redis.rpoplpush(
				'vycodi:queue:' + self.id,
				'vycodi:queue:' + self.id + ':working'
			)
		else:
			if timeout is None:
				timeout = 0
			taskId = self._redis.brpoplpush(
				'vycodi:queue:' + self.id,
				'vycodi:queue:' + self.id + ':working',
				timeout=timeout
			)
		task = self._taskLoader[taskId]

	def enqueue(self, task):
		self._redis.lpush('vycodi:queue:' + self.id, task.id)
		task.queue = self.id

	@staticmethod
	def getAll(cls, redis):
		queues = []
		for qId in redis.smembers('vycodi:queues'):
			queues.append(Queue(qId.decode('utf-8'), redis))
		return queues

	@staticmethod
	def get(cls, queueId, redis):
		redis.sadd(queueId)
		return Queue(queueId, redis)

	@staticmethod
	def enqueue(cls, task, redis):
		cls.get(task.queue, redis).enqueue(task)


class Batch(object):
	pass

class TaskLoaderException(Exception):
	pass


class LoaderNotSet(TaskLoaderException):
	def __init__(self):
		super(LoaderNotSet, self).__init__("LoaderNotSet")


class QueueException(Exception):
	pass


class QeueueNotSet(QueueException):
	def __init__(self):
		super(QeueueNotSet, self).__init__("QeueueNotSet")


class TaskLoader(object):
	def __init__(self, redis):
		self._redis = redis

	def __getitem__(self, key):
		if isinstance(key, Task):
			key = key.id
		elif not isinstance(key, int):
			key = int(key)
		taskDict = self._redis.hgetall('vycodi:task:' + str(key))
		if taskDict is None:
			raise KeyError()
		task = Task.fromRedisDict(taskDict, self)
		return task

	def enqueueTask(self, task, queue=None):
		if queue is not None:
			task.queue = queue
		if not task._registered:
			self.registerTask(task)
		Queue.enqueue(task, self._redis)

	def registerTask(self, task):
		if task._registered:
			return
		if task.id is None:
			task.id = self._fetchNextId()
		taskDict = task.exportRedis()
		keyBase = 'vycodi:task:' + str(task.id)
		self._redis.hmset(keyBase, taskDict)
		self._redis.rpush(keyBase + ':infiles', *task.inFiles)
		self._redis.rpush(keyBase + ':outfiles', *task.outFiles)
		task._registered = True

	def loadInFiles(self, task):
		if isinstance(task, Task):
			task = task.id
		return self._redis.lrange('vycodi:task:' + str(task) + ':infiles', 0, -1)

	def loadOutFiles(self, task):
		if isinstance(task, Task):
			task = task.id
		return self._redis.lrange('vycodi:task:' + str(task) + ':outfiles', 0, -1)

	def addInFile(self, task, file):
		if isinstance(task, Task):
			task = task.id
		if isinstance(file, File):
			file = file.id
		return self._redis.rpush('vycodi:task:' + str(task) + ':infiles', file)

	def addOutFile(self, task, file):
		if isinstance(task, Task):
			task = task.id
		if isinstance(file, File):
			file = file.id
		return self._redis.rpush('vycodi:task:' + str(task) + ':outfiles', file)

	def _fetchNextId(self):
		return self._redis.incr('vycodi:tasks:index')

class Task(object):
	def __init__(self, id=None, queue=None, processor=None, payload=None,
					batch=None, loader=None):
		self.id = id
		self.batch = batch
		self._queue = queue
		self.processor = processor
		self.payload = payload
		self._loader = loader
		self._inFiles = None
		self._outFiles = None
		self._registered = False

	@property
	def inFiles(self):
		if self._inFiles is None:
			if self._registered:
				if self._loader is not None:
					self._inFiles = self._loader.loadInFiles(self)
				else:
					raise LoaderNotSet()
			else:
				self._inFiles = []
		return self._inFiles

	@inFiles.setter
	def inFiles(self, inFiles):
		if self._registered:
			raise Exception("Can't set inFiles for registered task")
		else:
			self._inFiles = inFiles

	@property
	def outFiles(self):
		if self._outFiles is None:
			if self._registered:
				if self._loader is not None:
					self._outFiles = self._loader.loadOutFiles(self)
				else:
					raise LoaderNotSet()
			else:
				self._outFiles = []
		return self._outFiles

	@outFiles.setter
	def outFiles(self, outFiles):
		if self._registered:
			raise Exception("Can't set outFiles for registered task")
		else:
			self._outFiles = outFiles

	def register(self, loader=None):
		if loader is not None:
			self._loader = loader
		try:
			self._loader.register(self)
		except AttributeError:
			raise LoaderNotSet()

	def enqueue(self, queue=None, loader=None):
		if loader is not None:
			self._loader = loader
		try:
			self._loader.enqueueTask(self, queue=queue)
		except AttributeError:
			raise LoaderNotSet()

	def addInFile(self, file):
		if isinstance(file, File):
			file = file.id
		self.inFiles.append(file)
		if self._registered:
			try:
				self._loader.addInFile(task, file)
			except AttributeError:
				raise LoaderNotSet()

	def addOutFile(self, file):
		if isinstance(file, File):
			file = file.id
		self.outFiles.append(file)
		if self._registered:
			try:
				self._loader.addOutFile(task, file)
			except AttributeError:
				raise LoaderNotSet()

	def exportRedis(self):
		taskDict = dict()
		taskDict['id'] = self.id
		return taskDict

	@staticmethod
	def fromRedisDict(cls, taskDict, loader):
		taskDict = decodeRedis(taskDict)
		task = Task(
			id=int(taskDict['id']),
			processor=taskDict.get('processor', None),
			queue=taskDict.get('queue', None),
			batch=taskDict.get('batch', None),
			payload=taskDict.get('payload', None),
			loader=loader
		)
		task._registered = True


class ProcessingException(Exception):
	pass


class TaskProcessIntent(object):
	def __init__(self, task, worker):
		self._task = task
		self._worker = worker

	def __enter__(self):
		return self._task

	def __exit__(self, exc_type, exc_value, traceback):
		if issubclass(exc_type, ProcessingException):
			return True
		else:
			return False
