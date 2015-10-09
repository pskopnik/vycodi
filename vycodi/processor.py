from vycodi.queue import Failure, Task
from os.path import join
from importlib import import_module
import pkg_resources
import logging


class ProcessingException(Exception):
	def __init__(self, *args, requeue=True, **kwargs):
		super(ProcessingException, self).__init__(*args, **kwargs)
		self.requeue = requeue


class ProcessingManager(object):
	def __init__(self, worker, logger=None):
		self._worker = worker
		self._processorLoader = worker.processorLoader
		self._policy = worker.policy
		if logger is None:
			self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		else:
			self._logger = logger
		self._processors = {}

	def processTaskReservation(self, reservation):
		task = reservation.task
		try:
			proc = self._processorLoader.init(task.processor, cache=self._processors)
		except ImportError as e:
			self._logger.warn(
				"Couldn't import processor '%s' for task '%s': %s"
				% (task.processor, task.id, e))
			failure = Failure('UnknownProcessor', message=str(e))
			task.addFailure(failure)
			reservation.checkinFailed(failure)
		except ProcessingException as e:
			self._logger.warn(
				"ProcessingException during intialisation for task '%s': %s: %s"
				% (task.id, e.__class__.__name__, e))
			failure = Failure('ProcessingException', message="%s: %s" % (e.__class__.__name__, e))
			task.addFailure(failure)
			reservation.checkinFailed(failure, requeue=e.requeue)
		except Exception as e:
			self._logger.error(
				"Exception during intialisation for task '%s': %s: %s"
				% (task.id, e.__class__.__name__, e), exc_info=True)
			failure = Failure('InitException', message="%s: %s" % (e.__class__.__name__, e))
			task.addFailure(failure)
			reservation.checkinFailed(failure)

		try:
			proc.processTask(task)
		except ProcessingException as e:
			self._logger.warn(
				"ProcessingException during execution of task '%s': %s: %s"
				% (task.id, e.__class__.__name__, e))
			failure = Failure('ProcessingException', message="%s: %s" % (e.__class__.__name__, e))
			task.addFailure(failure)
			reservation.checkinFailed(failure, requeue=e.requeue)
		except Exception as e:
			self._logger.error(
				"Exception during execution of task '%s': %s: %s"
				% (task.id, e.__class__.__name__, e), exc_info=True)
			failure = Failure('Exception', message="%s: %s" % (e.__class__.__name__, e))
			task.addFailure(failure)
			reservation.checkinFailed(failure)
		else:
			self._logger.info(
				"Successfully processed task '%s' from queue '%s', processor '%s'"
				% (task.id, task.queue, task.processor))
			reservation.checkinFinished()

		self._worker.cleanupTaskDir(task)


class ClassWrapper(object):
	def __init__(self, name, cl):
		self.name = name
		self._cl = cl

	def load(self):
		return self._cl


class ProcessorLoader(object):
	def __init__(self, worker, namespace='vycodi.processors'):
		self.worker = worker
		self.namespace = namespace
		self.processors = {}
		self.fetchEntryPoints()

	def fetchEntryPoints(self):
		for eP in pkg_resources.iter_entry_points(self.namespace):
			self.processors[eP.name] = eP

	def load(self, name):
		try:
			cl = self.processors[name].load()
		except KeyError:
			try:
				lastDot = name.rfind('.')
				if lastDot == -1:
					raise IndexError()
				modName = name[:lastDot]
				clName = name[lastDot + 1:]
				module = import_module(modName)
				cl = getattr(module, clName)
				if not issubclass(cl, Processor):
					raise ImportError("Found class '%s' is not a Processor" % (name,))
				self.processors[name] = ClassWrapper(name, cl)
			except (AttributeError, IndexError):
				raise ImportError("No processor named '%s' available" % name)
		return cl

	def init(self, name, cache=None):
		procCl = self.load(name)
		try:
			return cache[procCl]
		except KeyError:
			proc = procCl(self.worker)
			cache[procCl] = proc
			return proc
		except TypeError:
			return procCl(self.worker)


class Processor(object):
	def __init__(self, worker):
		self._worker = worker

	def processTask(self, task):
		self.perform(
			*task.payload['args'],
			**task.payload['kwargs']
		)

	def perform(self, *args, **kwargs):
		pass

	@classmethod
	def enqueue(cls, queue, *args, **kwargs):
		try:
			procFullName = cls._procFullName
		except AttributeError:
			cls._procFullName = cls.__module__ + '.' + cls.__name__
			procFullName = cls._procFullName
		payload = {
			"args": args,
			"kwargs": kwargs
		}
		task = Task(processor=procFullName, payload=payload)
		queue.enqueue(task)
		return task


class FileProcessor(Processor):
	def __init__(self, worker):
		super(FileProcessor, self).__init__(worker)

	def processTask(self, task):
		inFiles, outFiles = self._prepareFiles(task)
		self.perform(
			*task.payload['args'],
			inFiles=inFiles,
			outFiles=outFiles,
			**task.payload['kwargs']
		)
		self._uploadFiles(outFiles)

	def _prepareFiles(self, task):
		fileLoader = self._worker.fileLoader
		self._worker.crtTaskDir(task)
		inFiles = []
		for fileId in task.inFiles:
			file = fileLoader[fileId]
			filePath = join(task.runDir, file.name)
			file.download(file=filePath)
			inFiles.append(file)
		outFiles = []
		for fileId in task.outFiles:
			file = fileLoader[fileId]
			file.path = join(task.runDir, file.name)
			outFiles.append(file)
		return inFiles, outFiles

	def _uploadFiles(self, outFiles):
		for file in outFiles:
			file.upload()

	def perform(self, *args, inFiles=None, outFiles=None, **kwargs):
		pass

	@classmethod
	def enqueue(cls, queue, inFiles=None, outFiles=None, *args, **kwargs):
		try:
			procFullName = cls._procFullName
		except AttributeError:
			cls._procFullName = cls.__module__ + '.' + cls.__name__
			procFullName = cls._procFullName
		payload = {
			"args": args,
			"kwargs": kwargs
		}
		task = Task(processor=procFullName, payload=payload)
		if inFiles is not None:
			task.inFiles = inFiles
		if outFiles is not None:
			task.outFiles = outFiles
		queue.enqueue(task)
		return task
