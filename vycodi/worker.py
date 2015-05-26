import logging

class Worker(object):
	def __init__(self, redis, id=None):
		self._redis = redis
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		if id is None:
			id = self._fetchNextId()
		self.id = id

	def start(self):
		pass

	def shutdown(self):
		pass

	def register(self):
		pass

	def unregister(self):
		pass

	def _fetchNextId(self):
		return self._redis.incr('vycodi:workers:index')
