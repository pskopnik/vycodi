import mattdaemon
import signal
import logging

class Daemon(mattdaemon.daemon):
	def __init__(self, *args, **kwargs):
		super(Daemon, self).__init__(*args, **kwargs)
		self._logFile = None
		self._logLevel = logging.INFO
		self._loggerName = None
		self._logger = None

	def run(self, *args, **kwargs):
		def handler(*args):
			self._shutdown()

		signal.signal(signal.SIGTERM, handler)
		self._setupLogging()
		self._run(*args, **kwargs)

	def _setupLogging(self):
		if self._logFile is not None:
			logging.basicConfig(level=self._logLevel, filename=self._logFile)
			name = self._loggerName or self.__class__.__name__
		self._logger = logging.getLogger(name)

	def wait(self):
		try:
			signal.pause()
		except KeyboardInterrupt:
			self._shutdown()
