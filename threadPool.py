#coding:utf8
#author:tntC4stl3

'''
本模块用于实现线程池
'''
import Queue
import threading
import time

class Worker(threading.Thread):
	def __init__(self, workQueue, resultQueue, timeout = 0):
		threading.Thread.__init__(self)
		self.id = Worker.worker_count
		Worker.worker_count += 1
		self.setDaemon(True)
		self.workQueue = workQueue
		self.resultQueue = resultQueue
		self.start()
	def run(self):
		while True:
			try:
				func, args, kwds = self.workQueue.get(timeout=self.timeout)
				res = func(*args, **kwds)
				self.resultQueue.put(res)
			except Queue.Empty:
				break
			except Exception, e:
				print e
		
class WorkManager(object):
	def __init__(self, threadNum, timeout = 1):
		self.threadNum = threadNum
		self.workQueue = Queue.Queue()
		self.resultQueue = Queue.Queue()
		self.threads = []
		self._recruitThread(threadNum)
	def _recruitThread(threadNum):
		for i in range(threadNum):
			worker = Worker(self.workQueue, self.resultQueue, self.timeout)
			self.thread.append(workder)
	def start(self):
		for w in self.workers:
			w.start()
	def wait_to_complete(self):
		# ...then, wait for each of them to terminate:
		while len(self.workers):
			worker = self.workers.pop()
			worker.join()
			if worker.isAlive() and not self.workQueue.empty():
				self.worker.append(worker)
		print "All jobs are complete"
	def add_job(self, func, *args, **kwds):
		self.workQueue.put((func, args. kwda))
	def get_result(self, *args, **kwds):
		return self.resultQueue.get(*args, **kwds)