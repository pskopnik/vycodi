from socketserver import ThreadingUnixStreamServer, ThreadingTCPServer, BaseRequestHandler
import socket

from jsonrpc import JSONRPCResponseManager, Dispatcher
from jsonrpc.exceptions import JSONRPCDispatchException

import time
import os
import json
import logging


def sendBytes(sock, payload):
	sock.sendall(payload.encode('utf-8') + b'\x00')


def sendObj(sock, payload):
	sendBytes(sock, json.dumps(payload, separators=(',', ':')))


def recvBytes(sock, bufSize=1024, timeout=None):
	if timeout is not None:
		start = time.perf_counter()
	respBytes = b''
	finished = False
	while not finished:
		chunk = sock.recv(bufSize)
		if not chunk:
			finished = True
		if b'\x00' in chunk:
			chunk = chunk[:-1]
			finished = True
		if timeout is not None and time.perf_counter() - start > timeout:
			raise socket.timeout()
		respBytes += chunk
	return respBytes.decode('utf-8')


def recvObj(sock, bufSize=1024, timeout=None):
	return json.loads(recvBytes(sock, bufSize=bufSize, timeout=timeout))


class RPCClient(object):
	def __init__(self, address, timeout=1, bufSize=1024):
		# Python's socketservers close the connection after each request
		# self._connected = False
		self._address = address
		# if isinstance(address, tuple):
		# 	self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		# else:
		# 	self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		# self._sock.settimeout(0.1)
		self._runningId = 0
		# self._respCache = dict()
		self._timeout = timeout
		self._bufSize = bufSize

	# def connect(self):
	# 	if not self._connected:
	# 		self._sock.connect(self._address)
	# 		self._connected = True

	# def disconnect(self):
	# 	if self._connected:
	# 		self._sock.close()
	# 		self._connected = False

	def sock(self):
		if isinstance(self._address, tuple):
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM).__enter__()
		else:
			sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM).__enter__()
		sock.settimeout(0.1)
		sock.connect(self._address)
		return sock

	def _sendRequest(self, sock, name, data):
		reqId = self._runningId
		self._runningId += 1
		payload = {
			"id": reqId,
			"jsonrpc": "2.0",
			"method": name,
			"params": data
		}
		sendObj(sock, payload)
		return reqId

	def _recvResponse(self, sock, reqId):
		response = recvObj(sock, bufSize=self._bufSize, timeout=self._timeout)
		return response

	# def _recvResponse(self, reqId):
	# 	if reqId in self._respCache:
	# 		result = self._respCache[reqId]
	# 		del self._respCache[reqId]
	# 		return result
	# 	while True:
	# 		response = recvObj(self._sock, bufSize=self._bufSize, timeout=self._timeout)
	# 		if response['id'] == reqId:
	# 			return response
	# 		else:
	# 			self._respCache[response['id']] = response


class RequestHandler(BaseRequestHandler):
	def __init__(self, *args, **kwargs):
		self._logger = logging.getLogger(__name__)
		super(RequestHandler, self).__init__(*args, **kwargs)

	def handle(self):
		request = recvBytes(self.request)
		self._logger.info("Received request")
		response = JSONRPCResponseManager.handle(request, self.dispatcher)
		sendBytes(self.request, response.json)
		if response.error is not None:
			self._logger.info("Received request, ERROR [%s] %s",
					response.error['code'], response.error['message'])


class TCPServer(ThreadingTCPServer):
	allow_reuse_address = True
	daemon_threads = True


class UnixServer(ThreadingUnixStreamServer):
	allow_reuse_address = True
	daemon_threads = True

	def shutdown(self):
		super(UnixServer, self).shutdown()
		try:
			os.unlink(self.server_address)
		except FileNotFoundError:
			pass


def Server(address, dispatcher, *args, **kwargs):
	class Handler(RequestHandler):
		pass

	Handler.dispatcher = dispatcher

	if isinstance(address, tuple):
		return TCPServer(address, Handler, *args, **kwargs)
	else:
		return UnixServer(address, Handler, *args, **kwargs)
