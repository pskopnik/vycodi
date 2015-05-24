import mattdaemon
import signal
import logging
import os

class Daemon(mattdaemon.daemon):
	def __init__(self, *args, **kwargs):
		super(Daemon, self).__init__(*args, **kwargs)
		self._logFile = None
		self._logLevel = logging.INFO
		self._loggerName = None
		self._logger = None
		self._recvdShutdownSignal = False

	def run(self, *args, **kwargs):
		def handler(sigNum, frame):
			if self._recvdShutdownSignal:
				return
			self._recvdShutdownSignal = True
			self._logger.info("Received shutdown signal %s", sigNum)
			try:
				self._shutdown()
			except Exception:
				self._logger.error("Exception during shutdown", exc_info=True)

		signal.signal(signal.SIGTERM, handler)
		self._setupLogging()
		try:
			self._run(*args, **kwargs)
		except Exception:
			self._logger.error("Exception during execution", exc_info=True)
			self._logger.error("Self destructing...")
			os.kill(os.getpid(), signal.SIGTERM)

	def _setupLogging(self):
		if self._logFile is not None:
			logging.basicConfig(level=self._logLevel, filename=self._logFile)
			name = self._loggerName or __name__ + '.' + self.__class__.__name__
		self._logger = logging.getLogger(name)

	def wait(self):
		try:
			signal.pause()
		except KeyboardInterrupt:
			self._shutdown()
