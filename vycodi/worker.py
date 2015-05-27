from vycodi.httpclient import FileLoader
from vycodi.daemon import Daemon
from vycodi.utils import redisFromConfig, storeJSONData, loadJSONData
from vycodi.queue import QueueWatcher, QueueTimeout
from vycodi.processor import ProcessorLoader, ProcessingManager
from tempfile import TemporaryDirectory
from os.path import join
from threading import Thread
import logging

class WorkerDaemon(Daemon):
	def __init__(self, worker, *args, logFile=None, **kwargs):
		super(WorkerDaemon, self).__init__(*args, **kwargs)
		self.worker = worker
		self._logFile = logFile

	def _run(self, *args, **kwargs):
		self.worker.start()
		self.wait()

	def _shutdown(self):
		self.worker.shutdown()
		self.worker.join()

	@classmethod
	def fromConfig(cls, config, *args, redis=None, **kwargs):
		runDir = abspath(config['runDir'])
		if not exists(runDir):
			mkdir(runDir)
		worker = Worker.fromConfig(config, redis=redis)
		pidFile = join(runDir, 'daemon.pid')
		logFile = join(runDir, 'daemon.log')
		return cls(worker, pidFile, *args, logFile=logFile, **kwargs)


class WorkerPool(object):
	def __init__(self):
		self.isInit = False

	def initPool(self, worker):
		self._worker = worker
		self.isInit = True

	def start(self):
		pass

	def shutdown(self):
		pass


class WorkerThreadPool(WorkerPool):
	def __init__(self, n=1):
		super(WorkerThreadPool, self).__init__()
		self._n = n
		self._threads = []

	def start(self):
		for i in range(self._n):
			thread = WorkerThread(self._worker)
			self._threads.append(thread)
			thread.start()

	def shutdown(self):
		for thread in self._threads:
			thread.signalStopIntent()
		for thread in self._threads:
			thread.join()
		self._threads = []


class WorkerThread(Thread):
	def __init__(self, worker):
		super(WorkerThread, self).__init__()
		self._logger = logging.getLogger(
			"%s.%s[%s]" % (__name__, self.__class__.__name, self.name))
		self._worker = worker
		self._processingManager = ProcessingManager(worker.processorLoader)
		self._shouldStop = False

	def signalStopIntent(self):
		self._shouldStop = True

	def run(self):
		while not self._shouldStop:
			try:
				task = worker.queueWatcher.reserveTask(timeout=5)
			except QueueTimeout:
				continue
			self._processingManager.processTask(task)

class Worker(object):
	def __init__(self, redis, id=None, queues=[], pool=None):
		self._redis = redis
		if pool is None:
			self._pool = WorkerThreadPool()
		else:
			self._pool = pool
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		if id is None:
			id = self._fetchNextId()
		self.id = id
		self._registered = False
		self._taskRunDirs = {}
		self.queueWatcher = QueueWatcher(redis, self, queues=queues)
		self.processorLoader = ProcessorLoader(redis)
		self.fileLoader = FileLoader(redis)
		# TODO sideeffect
		self.runDir = TemporaryDirectory()

	def start(self):
		self._logger.info("Starting...")
		if not self._pool.isInit:
			self._pool.initPool()
		self._pool.start()
		self._register()

	def shutdown(self):
		self._logger.info("Shutting down...")
		self._unregister()
		self._pool.shutdown()

	def crtTaskDir(self, task):
		if task.id in self._taskRunDirs:
			return self._taskRunDirs[task.id]
		path = join(self.runDir.name, 'task.%s' % task.id)
		task.runDir = path
		self._taskRunDirs[task.id] = path
		return path

	def _register(self):
		self._logger.info("Registering...")
		self._redis.hmset('vycodi:worker:' + str(self.id), {
			'address': self._address[0],
			'port': self._address[1]
		})
		self._redis.sadd('vycodi:workers', self.id)
		self._registered = True

	def _unregister(self):
		self._logger.info("Unregistering...")
		self.bucket.unregister()
		self._redis.srem('vycodi:workers', self.id)
		self._redis.delete('vycodi:worker:' + str(self.id))
		self._registered = False

	def _fetchNextId(self):
		return self._redis.incr('vycodi:workers:index')

	@classmethod
	def fromConfig(cls, config, redis=None):
		if redis is None:
			redis = redisFromConfig(config)
		runDir = abspath(config['runDir'])
		if not exists(runDir):
			mkdir(runDir)

		workerId = None
		try:
			workerId = loadJSONData(join(runDir, 'data.json'))['workerId']
		except FileNotFoundError:
			pass

		worker = cls(redis, id=workerId)

		if workerId is None:
			storeJSONData(join(runDir, 'data.json'), {'workerId': worker.id})
		return worker
