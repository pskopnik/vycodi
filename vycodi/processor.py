from os.path import join
from importlib import import_module

class ProcessingException(Exception):
	pass


def ProcessingManager(object):
	def __init__(self, processorLoader):
		self._processorLoader = processorLoader
		self._processors = {}

	def processTask(self, task):
		try:
			proc = self._processorLoader.init(task.processor, cache=self._processors)
		except ImportError as e:
			# TODO
			# Log error
			# Create new failure
			# Failure type = unknown processor
			# Store failure
			# requeue
			pass
		except ProcessingException as e:
			# TODO
			# Log info
			# Create new failure
			# Failure type = processing exception
			# Failure exception = exception message
			# Store failure
			# requeue conditionally
			pass
		except:
			# TODO
			# Log error with exception
			# Create new failure
			# Failure type = init exception
			# Failure exception = exception message
			# Store failure
			# requeue
			pass

		try:
			proc.processTask(task)
		except ProcessingException:
			# TODO
			# Log warn
			# Create new failure
			# Failure type = processing exception
			# Store failure
			# requeue
			pass
		except:
			# TODO
			# Log error with exception
			# Create new failure
			# Failure type = runtime exception
			# Failure exception = exception message
			# Store failure
			# requeue conditionally
			pass
		else:
			# TODO
			# Log info
			# Add to #queue:.:finished
			# Remove from #queue:.:working # lrem(name, value, num=-1)
			pass
		# TODO
		# Remove from #worker:.:working # lrem(name, value, num=-1)

		# TODO requeue
		# Set worker to None
		# If |failures| < threshold
		# 	Add to #queue
		# Else
		# 	Add to #queue:.:failed


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
				cl = getattr(name, clName)
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
			cls._procFullName = cls.__module__ + '.' cls.__name__
			procFullName = cls._procFullName
		payload = {
			"args": args,
			"kwargs": kwargs
		}
		task = Task(processor=procFullName, payload=payload)
		queue.enqueue(task)


class FileProcessor(Processor):
	def __init__(self, worker):
		super(FileProcessor, self).__init__(worker)

	def processTask(self, task):
		fileLoader = self.worker.fileLoader
		inFiles, outFiles = self._prepareFiles(task)
		self.perform(
			*task.payload['args'],
			inFiles=inFiles,
			outFiles=outFiles,
			**task.payload['kwargs']
		)
		self._uploadFiles(outFiles)

	def _prepareFiles(self, task):
		self.worker.crtTaskDir(task)
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

	def perform(self, *args, inFiles=[], outFiles=[], **kwargs):
		pass
