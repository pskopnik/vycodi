from __future__ import absolute_import
from daemon import DaemonContext
from daemon.pidfile import TimeoutPIDLockFile
import logging
import signal
import os
from os.path import abspath, join

try:
	ProcessLookupError = ProcessLookupError
	PY2 = False
	PY3 = True
except NameError:
	# Under Python 2, process lookup error is an OSError.
	import errno
	ProcessLookupError = OSError
	PY2 = True
	PY3 = False


class AlreadyRunning(Exception):
	pass


class NotRunning(Exception):
	pass


class Daemon(object):
	def __init__(self, runDir=None, pidFile=None, logFile=None,
			logLevel=logging.INFO):
		self._runDir = abspath(runDir) if runDir else os.getcwd()
		self._pidFile = pidFile or join(self._runDir, 'daemon.pid')
		self._logFile = logFile or join(self._runDir, 'daemon.log')
		self._logLevel = logLevel
		self._loggerName = None
		self.logger = None
		self._context = None

	def _setupLogging(self):
		if self._logLevel:
			name = self._loggerName or self.__class__.__module__ + '.' \
				+ self.__class__.__name__
			self.logger = logging.getLogger(name)
			self._fHandler = logging.FileHandler(self._logFile)
			self._fHandler.setFormatter(
				logging.Formatter(fmt="%(levelname)s:%(name)s:%(message)s")
			)
			self.logger.addHandler(self._fHandler)
			self.logger.setLevel(self._logLevel)

	def start(self, detachProcess=True):
		pidFile = TimeoutPIDLockFile(self._pidFile)

		context = DaemonContext(
			working_directory=self._runDir,
			umask=0o002,
			pidfile=pidFile,
			detach_process=detachProcess,
		)

		context.signal_map = {
			signal.SIGTERM: 'terminate',
			signal.SIGHUP: 'terminate',
			signal.SIGUSR1: 'terminate',
		}

		if self._isRunningAndBreak(pidFile):
			raise AlreadyRunning("PID file locked and process not stale")

		self._context = context
		try:
			context.open()
			self._setupLogging()
		except:
			if self.logger is None:
				self._setupLogging()
			self.logger.warn("Exception while entering context", exc_info=True)
			try:
				context.close()
			except:
				pass
			return

		try:
			self.run()
		except Exception as e:
			self.logger.error("Exception in run()", exc_info=e)
		finally:
			self.logger.info("Shutting down daemon")
			self.shutdown()
			try:
				self._fHandler.close()
			except:
				pass
			try:
				context.close()
			except:
				pass

	def run(self):
		"""
		To be overwritten by implementations.
		"""
		pass

	def shutdown(self):
		"""
		To be overwritten by implementations.
		"""
		pass

	def wait(self):
		"""
		Waits until a termination signal is received.
		"""
		while True:
			try:
				signal.pause()
			except (SystemExit, KeyboardInterrupt):
				return

	def stop(self):
		pidFile = TimeoutPIDLockFile(self._pidFile)

		if not self._isRunningAndBreak(pidFile):
			raise NotRunning()

		pid = pidFile.read_pid()
		try:
			os.kill(pid, signal.SIGTERM)
		except:
			pass

	def isRunning(self):
		pidFile = TimeoutPIDLockFile(self._pidFile)
		return self._isRunningAndBreak(pidFile)

	def _isRunningAndBreak(self, pidFile):
		if not pidFile.is_locked():
			return False

		pid = pidFile.read_pid()
		if pid is None:
			return False
		if self._isStale(pid):
			pidFile.break_lock()
			return False

		return True

	def _isStale(self, pid):
		try:
			os.kill(pid, signal.SIG_DFL)
		except ProcessLookupError as e:
			if PY3 or e.errno == errno.ESRCH:
				# Is stale
				return True

		# Is not stale
		return False
