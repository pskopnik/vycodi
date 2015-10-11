from threading import Thread
from time import sleep
import logging
from vycodi.utils import decodeRedis


class Purger(object):
	def purge(self, prefix, key, postfix, heartbeat):
		"""Called for set members whose main key expired, i.e. who died"""
		if heartbeat._redis.srem(heartbeat.setKey, key) == 1:
			self._purge(prefix, key, postfix, heartbeat)

	def _purge(self, prefix, key, postfix, heartbeat):
		pass

	def zombie(self, prefix, key, postfix, heartbeat):
		"""Called when the own main key expired"""
		pass


class Heartbeat(Thread):
	def __init__(self, redis, key, ttl, interval,
			prefix="", postfix="", value=None, setKey=None, purger=None):
		super(Heartbeat, self).__init__()
		self._logger = logging.getLogger(
			"%s.%s[%s][%s%s%s]" % (__name__, self.__class__.__name__, self.name,
				prefix, key, postfix)
		)
		self._redis = redis
		self.key = key
		self.ttl = ttl
		self.interval = interval
		self.prefix = prefix
		self.postfix = postfix
		self.value = value
		self.setKey = setKey
		self.purger = purger

	def run(self):
		self._logger.debug(
			"Setting initial heartbeat expiration")
		if self.value is None:
			self._redis.expire(
				self.prefix + self.key + self.postfix,
				self.ttl)
		else:
			self._redis.setex(
				self.prefix + self.key + self.postfix,
				self.value,
				self.ttl)
		self._shouldStopHeartbeat = False

		if self.setKey is not None and self.purger is not None:
			counter = 0
			maxCounter = self._redis.scard(self.setKey) * 5
		else:
			maxCounter = None

		while not self._shouldStopHeartbeat:
			sleep(self.interval)
			self._logger.debug("Sending heartbeat")
			r = self._redis.expire(
				self.prefix + self.key + self.postfix,
				self.ttl)
			if not r and self.purger is not None:
				self._logger.warn("Detected zombie")
				self.purger.zombie(self.prefix, self.key, self.postfix, self)

			if maxCounter is not None:
				counter += 1
				if counter >= maxCounter:
					self._redis.sadd(self.setKey, self.key)
					self._logger.debug("Checking for dead instances")
					for k in self._redis.smembers(self.setKey):
						k = decodeRedis(k)
						if not self._redis.exists(self.prefix + str(k) + self.postfix):
							self._logger.info(
								"Found dead instance '%s' + '%s' + '%s'"
								% (self.prefix, k, self.postfix)
							)
							self.purger.purge(self.prefix, k, self.postfix, self)
					counter = 0

	def signalStopIntent(self):
		self._shouldStopHeartbeat = True
