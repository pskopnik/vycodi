from vycodi.utils import decodeRedis, loadJSONField, storeJSONField, dumpJSON, loadJSON
from vycodi.httpclient import File
from queue import Empty
import time


class QueueTimeout(Empty):
	def __init__(self):
		super(QueueTimeout, self).__init__()


class Queue(object):
	_queuesCache = {}

	def __init__(self, id, redis, taskLoader=None):
			self.id = id
			self._redis = redis
			if taskLoader is None:
				self._taskLoader = TaskLoader(redis)
			else:
				self._taskLoader = taskLoader

	def reserveTask(self, worker, timeout=0):
		"""Fetches and reserves the next queue in the task for the
		passed in worker
		timeout value resembles socket.socket.settimeout()
		Returns a TaskReservation object
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
		reservation = TaskReservation(self, task, worker)
		return reservation

	def enqueue(self, task):
		task.queue = self.id
		self._taskLoader.registerTask(task)
		self._redis.lpush('vycodi:queue:' + str(self.id), task.id)

	def removeTaskFromWorking(self, task):
		self._redis.lrem('vycodi:queue:' + str(self.id) + ':working', -1, task.id)

	def removeTaskFromWorkerWorking(self, task):
		self._redis.lrem('vycodi:worker:' + str(task.worker) + ':working', -1, task.id)

	def addTaskToFinished(self, task):
		self._redis.lpush('vycodi:queue:' + str(self.id) + ':finished', task.id)

	def addTaskToFailed(self, task):
		self._redis.lpush('vycodi:queue:' + str(self.id) + ':failed', task.id)

	@classmethod
	def getAll(cls, redis):
		queues = []
		for qId in redis.smembers('vycodi:queues'):
			queues.append(Queue(qId.decode('utf-8'), redis))
		return queues

	@classmethod
	def get(cls, queueId, redis):
		try:
			specCache = cls._queuesCache[redis]
		except KeyError:
			cls._queuesCache[redis] = {}
			specCache = cls._queuesCache[redis]
		try:
			return specCache[queueId]
		except KeyError:
			redis.sadd('vycodi:queues', queueId)
			queue = Queue(queueId, redis)
			specCache[queueId] = queue
			return queue


class QueueWatcher(object):
	def __init__(self, redis, worker, queues=[], taskLoader=None):
		self._worker = worker
		self._redis = redis
		self._queues = []
		self._taskLoader = taskLoader
		for queue in queues:
			if not isinstance(queue, Queue):
				queue = Queue.get(queue, self._redis, taskLoader=self._taskLoader)
			self._queues.append(queue)

	def addQueue(self, queue):
		if not isinstance(queue, Queue):
			queue = Queue.get(queue, self._redis, taskLoader=self._taskLoader)
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


class TaskReservation(object):
	def __init__(self, queue, task, worker):
		self.queue = queue
		self.task = task
		self.worker = worker
		self._policy = worker.policy

	def checkinFinished(self):
		if not self.worker.isAlive():
			return
		if self._policy.storeFinishedTask(self.task):
			self.queue.addTaskToFinished(self.task)
		self.queue.removeTaskFromWorking(self.task)
		self.queue.removeTaskFromWorkerWorking(self.task)

	def checkinFailed(self, failure, requeue=True):
		if not self.worker.isAlive():
			return
		if requeue and self._policy.requeueAfterFailure(self.task, failure):
			self._requeue()
		else:
			if self._policy.storeFailedTask(self.task, failure):
				self.queue.addTaskToFailed(self.task)
		self.queue.removeTaskFromWorking(self.task)
		self.queue.removeTaskFromWorkerWorking(self.task)

	def _requeue(self):
		self.task.worker = None
		# TODO # CRASH
		self.queue.enqueue(self.task)


class Batch(object):
	pass


class TaskLoaderException(Exception):
	pass


class LoaderNotSet(TaskLoaderException):
	def __init__(self):
		super(LoaderNotSet, self).__init__("LoaderNotSet")


class QueueException(Exception):
	pass


class QueueNotSet(QueueException):
	def __init__(self):
		super(QueueNotSet, self).__init__("QueueNotSet")


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
		if task._loader is None:
			task._loader = self
		if task.id is None:
			task.id = self._fetchNextId()
		taskDict = task.exportRedis()
		keyBase = self.keyBase + str(task.id)
		self._redis.hmset(keyBase, taskDict)
		if not len(task.inFiles) == 0:
			self._redis.rpush(keyBase + ':infiles', *task.inFiles)
		if not len(task.outFiles) == 0:
			self._redis.rpush(keyBase + ':outfiles', *task.outFiles)
		if not len(task.failures) == 0:
			self._redis.rpush(keyBase + ':failures',
				*[dumpJSON(f.exportRedis()) for f in task.failures])
		task._registered = True

	def loadFailures(self, task):
		if isinstance(task, Task):
			taskObj = task
			task = task.id
		else:
			taskObj = self[task]
		failures = []
		for failureJSON in self._redis.lrange(self.keyBase + str(task) + ':failures', 0, -1):
			failureDict = loadJSON(failureJSON)
			failures.append(Failure.fromDict(failureDict, taskObj))

		return failures

	def addFailure(self, task, failure):
		if isinstance(task, Task):
			task = task.id
		self._redis.rpush(
			self.keyBase + str(task) + ':failures',
			dumpJSON(failure.exportRedis())
		)

	def loadInFiles(self, task):
		if isinstance(task, Task):
			task = task.id
		return decodeRedis(self._redis.lrange(self.keyBase + str(task) + ':infiles', 0, -1))

	def loadOutFiles(self, task):
		if isinstance(task, Task):
			task = task.id
		return decodeRedis(self._redis.lrange(self.keyBase + str(task) + ':outfiles', 0, -1))

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

	def loadResult(self, task):
		if isinstance(task, Task):
			task = task.id
		return decodeRedis(self._redis.hgetall(self.keyBase + str(task) + ':result'))

	def storeResult(self, task, result):
		if isinstance(task, Task):
			task = task.id
		self._redis.hmset(self.keyBase + str(task) + ':result', result)

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
		self.__failures = None
		self.__result = None
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
	def failures(self):
		if self.__failures is None:
			if self._registered:
				if self._loader is not None:
					self.__failures = self._loader.loadFailures(self)
				else:
					raise LoaderNotSet()
			else:
				self.__failures = []
		return self.__failures

	@failures.setter
	def failures(self, failures):
		raise Exception("Can't set failures for task")

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

	@property
	def result(self):
		if self.__result is None:
			if self._registered:
				if self._loader is not None:
					self.__result = self._loader.loadResult(self)
				else:
					raise LoaderNotSet()
			else:
				self.__result = {}
		return self.__result

	@result.setter
	def result(self, result):
		if self._registered:
			if self._loader is not None:
				self._loader.storeResult(self, result)
			else:
				raise LoaderNotSet()
		self.__result = result

	def enqueue(self, queue=None, loader=None):
		if loader is not None:
			self._loader = loader
		try:
			self._loader.enqueueTask(self, queue=queue)
		except AttributeError:
			raise LoaderNotSet()

	def addFailure(self, failure):
		failure.task = self
		self.failures.append(failure)
		if self._registered:
			try:
				self._loader.addFailure(self, failure)
			except AttributeError:
				raise LoaderNotSet()

	def addInFile(self, file):
		if isinstance(file, File):
			file = file.id
		self.inFiles.append(file)
		if self._registered:
			try:
				self._loader.addInFile(self, file)
			except AttributeError:
				raise LoaderNotSet()

	def addOutFile(self, file):
		if isinstance(file, File):
			file = file.id
		self.outFiles.append(file)
		if self._registered:
			try:
				self._loader.addOutFile(self, file)
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
		task = cls(
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


class Failure(object):
	def __init__(self, type, message=None, task=None):
		self.type = type
		self.message = message
		self.task = task

	def exportRedis(self):
		return {
			'type': self.type,
			'message': self.message
		}

	@classmethod
	def fromDict(cls, failureDict, task):
		failure = cls(
			failureDict['type'],
			message=failureDict['message'],
			task=task
		)
		return failure
