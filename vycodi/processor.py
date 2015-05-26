from os.path import join
from importlib import import_module

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
				self.processors[name] = ClassWrapper(name, cl)
			except (AttributeError, IndexError):
				raise ImportError("No processor named '%s' available" % name)
		if not issubclass(cl, Processor):
			raise ImportError("Found class '%s' not a Procesor" % (name,))
		return cl

	def init(self, name):
		return self.load(name)(self.worker)


class Processor(object):
	def __init__(self, worker):
		self._worker = worker

	def processTask(self, task):
		with task.processIntent():
			self.perform(
				*task.payload['args'],
				**task.payload['kwargs']
			)

	def perform(self, *args, **kwargs):
		pass


class FileProcessor(Processor):
	def __init__(self, worker):
		super(FileProcessor, self).__init__(worker)

	def processTask(self, task):
		fileLoader = self.worker.fileLoader
		with task.processIntent():
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
