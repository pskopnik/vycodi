import requests

class HTTPClientException(Exception):
	def __init__(self, retCode, message):
		super(HTTPClientException, self).__init__()
		self.retCode = retCode
		self.message = message

class ClientPool(object):
	def __init__(self):
		self._clients = dict()

	def __getitem__(self, key):
		if isinstance(key, tuple):
			key = key[0] + ':' + str(key[1])
		if key not in self._clients:
			self.add(Client(key))
		return self._clients[key]

	def add(self, client):
		serverStrAdr = client.serverStrAdr
		if serverStrAdr in self._clients:
			del self._clients[serverStrAdr]
		self._clients[serverStrAdr] = client

class Client(object):
	def __init__(self, server):
		if isinstance(server, tuple):
			self.serverAdr = server
			self.serverStrAdr = server[0] + ':' + str(server[1])
		else:
			self.serverStrAdr = server
			if ":" not in server:
				self.serverAdr = (server, 80)
			else:
				adrList = server.split(":")
				self.serverAdr = (adrList[0], int(adrList[1]))
		self._s = requests.Session()
		self.baseUrl = 'http://' + self.serverStrAdr + '/'

	def download(self, id, outF):
		r = self._s.get(self.baseUrl + str(id), stream=True)
		if not r.status_code == requests.codes.ok:
			raise HTTPClientException(r.status_code, r.text)
		if isinstance(outF, str):
			with open(outF, 'wb') as outFO:
				for chunk in r.iter_content(chunk_size=1024 * 1024):
					if chunk:
						outFO.write(chunk)
				outFO.flush()
		else:
			for chunk in r.iter_content(chunk_size=1024 * 1024):
				if chunk:
					outF.write(chunk)
			outF.flush()

	def upload(self, id, inF):
		if isinstance(inF, str):
			with open(inF, 'rb') as inFO:
				r = self._s.post(self.baseUrl + str(id), data=inFO)
		else:
			r = self._s.post(self.baseUrl + str(id), data=inF)
		if not r.status_code == requests.codes.ok:
			raise HTTPClientException(r.status_code, r.text)
