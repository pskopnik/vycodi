from vycodi.utils import decodeRedis, loadJSONField, storeJSONField
from vycodi.httpclient import File
import queue
import time

class QueueTimeout(queue.Empty):
	def __init__(self):
		super(QueueTimeout, self).__init__()


class Queue(object):
	def __init__(self, id, redis):
			self.id = id
			self._redis = redis
			self._taskLoader = TaskLoader(redis)

	def reserveTask(self, worker, timeout=0):
		"""Fetches and reserves the next queue in the task for the
		passed in worker
		timeout value resembles socket.socket.settimeout()
		If
		"""
		if timeout is 0:
			taskId = self._redis.rpoplpush(
				'vycodi:queue:' + str(self.id),
				'vycodi:queue:' + str(self.id) + ':working'
			)
		else:
			if timeout is None:
				timeout = 0
			if not (isinstance(timeout, int)
				or (isinstance(timeout, float) and timeout.is_integer())):
				raise TypeError('timeout must be an integer value')
			taskId = self._redis.brpoplpush(
				'vycodi:queue:' + str(self.id),
				'vycodi:queue:' + str(self.id) + ':working',
				timeout=timeout
			)
		if taskId is None:
			raise QueueTimeout()
		task = self._taskLoader[taskId]
		task.worker = worker.id
		self._redis.lpush('vycodi:worker:' + str(worker.id) + ':working', task.id)
		return task

	def enqueue(self, task):
		task.queue = self.id
		self._taskLoader.registerTask(task)
		self._redis.lpush('vycodi:queue:' + str(self.id), task.id)

	@classmethod
	def getAll(cls, redis):
		queues = []
		for qId in redis.smembers('vycodi:queues'):
			queues.append(Queue(qId.decode('utf-8'), redis))
		return queues

	@classmethod
	def get(cls, queueId, redis):
		redis.sadd('vycodi:queues', queueId)
		return Queue(queueId, redis)

	@classmethod
	def enqueue(cls, task, redis):
		cls.get(task.queue, redis).enqueue(task)


class QueueWatcher(object):
	def __init__(self, redis, worker, queues=[]):
		self._worker = worker
		self._redis = redis
		self._queues = []
		for queue in queues:
			if not isinstance(queue, Queue):
				queue = Queue.get(queue, self._redis)
			self._queues.append(queue)

	def addQueue(self, queue):
		if not isinstance(queue, Queue):
			queue = Queue.get(queue, self._redis)
		self._queues.append(queue)

	def reserveTask(self, timeout=None, wait=0.1):
		if timeout is not None:
			start = time.perf_counter()
		while True:
			try:
				return self._fetchFromQueues(self._worker)
			except QueueTimeout:
				if timeout is not None and time.perf_counter() > start + timeout:
					raise
				time.sleep(wait)

	def _fetchFromQueues(self, worker):
		"""Tries to reserve a task from any queue (in self._queues)
		"""
		for queue in self._queues:
			try:
				return queue.reserveTask(self._worker)
			except QueueTimeout:
				pass
		raise QueueTimeout()


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
		if not len(self.task.inFiles) == 0:
			self._redis.rpush(keyBase + ':infiles', *task.inFiles)
		if not len(self.task.outFiles) == 0:
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
				try:
					data[arg] = taskExp[arg]
				except KeyError:
					pass
			if len(data) == 0:
				return
		self._redis.hmset(self.keyBase + str(task.id), data)

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
					self._loader.updateTask(self, key)
				except AttributeError:
					raise LoaderNotSet()
		else:
			super(Task, self).__setattr__(key, value)

	def register(self, loader=None):
		if loader is not None:
			self._loader = loader
		try:
			self._loader.registerTask(self)
		except AttributeError:
			raise LoaderNotSet()

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
			fileIds = []
			for file in inFiles:
				if isinstance(file, File):
					fileIds.append(file.id)
				else:
					fileIds.append(file)
			self.__inFiles = fileIds

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
			fileIds = []
			for file in outFiles:
				if isinstance(file, File):
					fileIds.append(file.id)
				else:
					fileIds.append(file)
			self.__outFiles = fileIds

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

	@classmethod
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
		return task
