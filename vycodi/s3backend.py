import botocore
from boto3.session import Session
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from vycodi.bucket import File


class S3File(File):
	def __init__(self, id, name, key, type, bucket=None):
		super(S3File, self).__init__(id, name, type, bucket=bucket)
		self.key = key

	def export(self):
		return {
			"id": self.id,
			"name": self.name,
			"key": self.key,
			"type": self._type
		}

	@classmethod
	def fromDict(cls, fileDict, bucket=None):
		return cls(
			fileDict["id"],
			fileDict["name"],
			fileDict["key"],
			fileDict["type"],
			bucket=bucket
		)


class S3Backend(object):
	fileClass = S3File

	def __init__(self, keyId, accessKey, region, bucketName):
		self._session = Session(
			aws_access_key_id=keyId,
			aws_secret_access_key=accessKey,
			region_name=region
		)
		self._s3 = self._session.resource('s3')
		self._bucket = self._s3.Bucket(bucketName)
		self._crtBucketIfNotExists()

	def openR(self, file):
		fileObject = self._bucket.Object(file.key)
		return fileObject.get()['Body']

	def openW(self, file, contentLength=None):
		fileObject = self._bucket.Object(file.key)
		return S3UploadStream(
			self._s3.meta.client,
			self._bucket,
			fileObject,
			contentLength)

	def genReadURL(self, file):
		fileObject = self._bucket.Object(file.key)
		return self._s3.meta.client.generate_presigned_url(
			'get_object',
			Params={
				'Bucket': self._bucket.name,
				'Key': fileObject.key
			},
			ExpiresIn=300
		)

	def size(self, file):
		fileObject = self._bucket.Object(file.key)
		return fileObject.content_length

	def contentType(self, file):
		fileObject = self._bucket.Object(file.key)
		return fileObject.content_type

	def lastModified(self, file):
		fileObject = self._bucket.Object(file.key)
		return fileObject.last_modified.timestamp()

	def _crtBucketIfNotExists(self):
		exists = True
		try:
			self._s3.meta.client.head_bucket(Bucket=self._bucket.name)
		except botocore.exceptions.ClientError as e:
			error_code = int(e.response['Error']['Code'])
			if error_code == 404:
				exists = False

		if not exists:
			self._s3.create_bucket(Bucket=self._bucket.name)

	@classmethod
	def fromConfig(cls, config):
		return cls.fromBackendConfig(config.get('backend', {}))

	@classmethod
	def fromBackendConfig(cls, config):
		return cls(
			config['keyId'],
			config['accessKey'],
			config['region'],
			config['bucketName']
		)


class S3UploadStream(object):
	pool = ThreadPoolExecutor(5)
	_partSize = 5 << 22
	_multipartUploadThreshold = 5 << 23

	def __init__(self, client, bucket, fileObj, contentLength):
		self._client = client
		self._bucket = bucket
		self._fileObj = fileObj
		if contentLength is not None:
			self._contentLength = contentLength
			self._useMultipart = contentLength > self._multipartUploadThreshold
		else:
			self._useMultipart = True
		self._size = 0
		self._buffer = BytesIO()
		if self._useMultipart:
			self._curPartNumber = 0
			self._uploadFtrs = []
			response = self._client.create_multipart_upload(
				Bucket=bucket.name,
				Key=fileObj.key
			)
			self._uploadId = response['UploadId']

	def write(self, bytes):
		self._size += len(bytes)
		self._buffer.write(bytes)
		if self._useMultipart and self._size >= self._partSize:
			self._uploadPart()

	def close(self):
		if self._useMultipart:
			self._completeUpload()
		else:
			self._buffer.seek(0)
			self._bucket.put_object(Key=self._fileObj.key, Body=self._buffer)

	def _uploadPart(self):
		print("uploadPart")
		partBuffer = self._buffer
		partBuffer.seek(0)
		self._buffer = BytesIO()
		self._size = 0
		self._curPartNumber += 1
		ftr = self.pool.submit(self._doPartUpload, partBuffer, self._curPartNumber)
		self._uploadFtrs.append(ftr)

	def _doPartUpload(self, partBuffer, partNumber):
		try:
			response = self._client.upload_part(
				Bucket=self._bucket.name, Key=self._fileObj.key,
				UploadId=self._uploadId, PartNumber=partNumber,
				Body=partBuffer
			)
		except Exception as e:
			print(e)
			raise
		del partBuffer
		return {'ETag': response['ETag'], 'PartNumber': partNumber}

	def _completeUpload(self):
		parts = []
		for future in self._uploadFtrs:
			parts.append(future.result())
		self._client.complete_multipart_upload(
			Bucket=self._bucket.name, Key=self._fileObj.key,
			UploadId=self._uploadId,
			MultipartUpload={'Parts': parts}
		)
