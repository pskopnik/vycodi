from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from threading import Thread
from vycodi.bucket import BackendError
import logging

__version__ = "0.2"


class ThreadingServer(ThreadingMixIn, HTTPServer):
	pass


class Server(Thread):
	def __init__(self, address, bucket):
		super(Server, self).__init__()

		class Handler(HTTPRequestHandler):
			pass

		Handler.bucket = bucket
		self._server = ThreadingServer(address, Handler)
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

	def run(self):
		self._logger.info("Starting...")
		self._server.serve_forever()

	def shutdown(self):
		self._logger.info("Shutting down...")
		self._server.shutdown()


class HTTPRequestHandler(SimpleHTTPRequestHandler):

	"""Simple HTTP request handler with GET and HEAD commands.

	This serves files from the current directory and any of its
	subdirectories.  The MIME type for files is determined by
	calling the .guess_type() method.

	The GET and HEAD requests are identical except that the HEAD
	request omits the actual contents of the file.

	"""

	server_version = "vycodiHTTP/" + str(__version__)
	error_content_type = "text/plain"
	error_message_format = "[%(code)d] %(message)s - %(explain)s"
	buffer_size = 1024 * 1024

	def __init__(self, *args, **kwargs):
		self._logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
		super(HTTPRequestHandler, self).__init__(*args, **kwargs)

	def do_GET(self):
		"""Serve a GET request."""
		f = self.send_head()
		if f:
			try:
				self.copyfile(f, self.wfile)
			finally:
				f.close()

	def do_HEAD(self):
		"""Serve a HEAD request."""
		f = self.send_head()
		if f:
			f.close()

	def do_POST(self):
		"""Serve a POST request."""
		r = self.do_upload()
		if r:
			self.send_response(200)
			self.send_header("Content-Type", "text/plain")
			self.send_header("Content-Length", 0)
			self.end_headers()

	def do_upload(self):
		contentLength = int(self.headers['Content-Length'])

		fileId = self.path.lstrip('/')
		try:
			fileObj = self.bucket[fileId]
			if not fileObj.writable():
				self.send_error(403, explain="File not writable")
				return False
		except KeyError:
			self.send_error(404)
			return False

		try:
			self.log_message("Starting upload of %s - %s", fileId, fileObj.name)
			f = fileObj.openW(contentLength=contentLength)
			while contentLength > 0:
				if contentLength < self.buffer_size:
					chunk = self.rfile.read(contentLength)
				else:
					chunk = self.rfile.read(self.buffer_size)
				f.write(chunk)
				contentLength -= len(chunk)
			self.log_message("Finished upload of %s", fileId)
			return True
		except BackendError as e:
			try:
				f.close()
			except:
				pass
			self.log_error("BackendError: %s", str(e))
			self.send_error(500, explain="Backend error")
			return False
		finally:
			f.close()

	def send_head(self):
		"""Common code for GET and HEAD commands.

		This sends the response code and MIME headers.

		Return value is either a file object (which has to be copied
		to the outputfile by the caller unless the command was HEAD,
		and must be closed by the caller under all circumstances), or
		None, in which case the caller has nothing further to do.

		"""
		fileId = self.path.lstrip('/')
		try:
			fileObj = self.bucket[fileId]
			if not fileObj.readable():
				self.send_error(403, explain="File not readable")
				return None
		except KeyError:
			self.send_error(404)
			return None
		url = fileObj.genReadURL()
		if url is not None:
			self.send_response(302)
			self.send_header("Location", url)
			return None
		try:
			f = fileObj.openR()
		except BackendError as e:
			try:
				f.close()
			except:
				pass
			self.log_error("BackendError: %s", str(e))
			self.send_error(500, explain="Backend error")
			return None
		try:
			self.log_message("Sending headers for %s - %s", fileId, fileObj.name)
			self.send_response(200)
			self.send_header("Content-Type", fileObj.contentType())
			self.send_header("Content-Length", fileObj.size())
			self.send_header("Last-Modified", self.date_time_string(fileObj.lastModified()))
			self.end_headers()
			return f
		except:
			f.close()
			raise

	def log_message(self, format, *args):
		self._logger.info("%s - - [%s] %s" %
						(self.address_string(),
						self.log_date_time_string(),
						format % args))

	def log_error(self, format, *args):
		self._logger.error("%s - - [%s] %s" %
						(self.address_string(),
						self.log_date_time_string(),
						format % args))
