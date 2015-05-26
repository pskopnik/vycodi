from vycodi.utils import decodeRedis, loadJSONField, storeJSONField
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
		task.worker = worker.id
		self._redis.lpush('vycodi:worker:' + worker.id + ':tasks', worker.id)

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
	keyBase = 'vycodi:task:'

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
		keyBase = self.keyBase + str(task.id)
		self._redis.hmset(keyBase, taskDict)
		self._redis.rpush(keyBase + ':infiles', *task.inFiles)
		self._redis.rpush(keyBase + ':outfiles', *task.outFiles)
		task._registered = True

	def loadInFiles(self, task):
		if isinstance(task, Task):
			task = task.id
		return self._redis.lrange(self.keyBase + str(task) + ':infiles', 0, -1)

	def loadOutFiles(self, task):
		if isinstance(task, Task):
			task = task.id
		return self._redis.lrange(self.keyBase + str(task) + ':outfiles', 0, -1)

	def addInFile(self, task, file):
		if isinstance(task, Task):
			task = task.id
		if isinstance(file, File):
			file = file.id
		return self._redis.rpush(self.keyBase + str(task) + ':infiles', file)

	def addOutFile(self, task, file):
		if isinstance(task, Task):
			task = task.id
		if isinstance(file, File):
			file = file.id
		return self._redis.rpush(self.keyBase + str(task) + ':outfiles', file)

	def updateTask(self, task, *args):
		taskExp = task.exportRedis()
		if len(args) == 0:
			data = taskExp
		else:
			data = dict()
			for arg in args:
				data = taskExp[arg]
		self._redis.hmset(self.keyBase + task.id, data)

	def _fetchNextId(self):
		return self._redis.incr('vycodi:tasks:index')

class Task(object):
	def __init__(self, id=None, queue=None, worker=None, processor=None,
					payload=None, batch=None, loader=None):
		self._id = id
		self._queue = queue
		self._worker = worker
		self._batch = batch
		self._processor = processor
		self._payload = payload
		self._loader = loader
		self.__inFiles = None
		self.__outFiles = None
		self._registered = False

	def __getattr__(self, key):
		if not key.startswith('_'):
			return self.__getattribute__('_' + key)
		else:
			return self.__getattribute__(key)

	def __setattr__(self, key, value):
		if not key.startswith('_') and hasattr(self, '_' + key):
			super(Task, self).__setattr__('_' + key, value)
			if self._registered:
				try:
					self._loader.updateTask(self, (key,))
				except AttributeError:
					raise LoaderNotSet()
		else:
			super(Task, self).__setattr__(key, value)

	@property
	def inFiles(self):
		if self.__inFiles is None:
			if self._registered:
				if self._loader is not None:
					self.__inFiles = self._loader.loadInFiles(self)
				else:
					raise LoaderNotSet()
			else:
				self.__inFiles = []
		return self.__inFiles

	@inFiles.setter
	def inFiles(self, inFiles):
		if self._registered:
			raise Exception("Can't set inFiles for registered task")
		else:
			self.__inFiles = inFiles

	@property
	def outFiles(self):
		if self.__outFiles is None:
			if self._registered:
				if self._loader is not None:
					self.__outFiles = self._loader.loadOutFiles(self)
				else:
					raise LoaderNotSet()
			else:
				self.__outFiles = []
		return self.__outFiles

	@outFiles.setter
	def outFiles(self, outFiles):
		if self._registered:
			raise Exception("Can't set outFiles for registered task")
		else:
			self.__outFiles = outFiles

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
		taskDict['id'] = self._id
		taskDict['queue'] = self._queue
		taskDict['worker'] = self._worker
		if self._processor is not None:
			taskDict['processor'] = self._processor
		if self._batch is not None:
			taskDict['batch'] = self._batch
		if self._payload is not None:
			storeJSONField(taskDict, 'payload', self._payload)
		return taskDict

	@staticmethod
	def fromRedisDict(cls, taskDict, loader):
		taskDict = decodeRedis(taskDict)
		task = Task(
			id=int(taskDict['id']),
			queue=taskDict['queue'],
			worker=taskDict.get('worker', None),
			processor=taskDict.get('processor', None),
			batch=taskDict.get('batch', None),
			payload=loadJSONField(taskDict, 'payload', default={}),
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
		if exc_type is None:
			pass
		else:
			if issubclass(exc_type, ProcessingException):
				return True
			else:
				return False
