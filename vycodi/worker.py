from vycodi.httpclient import FileLoader
from vycodi.daemon import Daemon
from vycodi.utils import redisFromConfig, storeJSONData, loadJSONData
from tempfile import TemporaryDirectory
from os.path import join
from threading import Thread
import logging

class WorkerDaemon(Daemon):
	def __init__(self, host, *args, logFile=None, **kwargs):
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


class Worker(object):
	def __init__(self, redis, id=None):
		self._redis = redis
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		if id is None:
			id = self._fetchNextId()
		self.id = id
		self.fileLoader = FileLoader(redis)
		self.runDir = TemporaryDirectory()
		self._taskRunDirs = {}

	def start(self):
		pass

	def shutdown(self):
		pass

	def register(self):
		pass

	def unregister(self):
		pass

	def crtTaskDir(self, task):
		if task.id in self._taskRunDirs:
			return self._taskRunDirs[task.id]
		path = join(self.runDir.name, 'task.%s' % task.id)
		task.runDir = path
		self._taskRunDirs[task.id] = path
		return path

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


class WorkerThread(Thread):
	def __init__(self, worker):
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		self._worker = worker
