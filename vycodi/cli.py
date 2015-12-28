import argh
from vycodi.utils import loadJSONConfig
from vycodi.host import HostDaemon
from vycodi.worker import WorkerDaemon


@argh.named("start")
def startHost(configFile, foreground=False):
	config = loadJSONConfig(configFile)
	hostDaemon = HostDaemon.fromConfig(config)
	hostDaemon.start(detachProcess=not foreground)


@argh.named("stop")
def stopHost(configFile):
	config = loadJSONConfig(configFile)
	hostDaemon = HostDaemon.fromConfig(config)
	hostDaemon.stop()


@argh.named("status")
def statusHost(configFile):
	config = loadJSONConfig(configFile)
	hostDaemon = HostDaemon.fromConfig(config)
	if hostDaemon.isRunning():
		print("Host daemon running")
	else:
		print("Host daemon not running")


@argh.named("start")
def startWorker(configFile, foreground=False):
	config = loadJSONConfig(configFile)
	workerDaemon = WorkerDaemon.fromConfig(config)
	workerDaemon.start(detachProcess=not foreground)


@argh.named("stop")
def stopWorker(configFile):
	config = loadJSONConfig(configFile)
	workerDaemon = WorkerDaemon.fromConfig(config)
	workerDaemon.stop()


@argh.named("status")
def statusWorker(configFile):
	config = loadJSONConfig(configFile)
	workerDaemon = WorkerDaemon.fromConfig(config)
	if workerDaemon.isRunning():
		print("Worker daemon running")
	else:
		print("Worker daemon not running")

parser = argh.ArghParser()
parser.add_commands((startHost, stopHost, statusHost), namespace="host")
parser.add_commands((startWorker, stopWorker, statusWorker), namespace="worker")


def main():
	parser.dispatch()

if __name__ == '__main__':
	main()
