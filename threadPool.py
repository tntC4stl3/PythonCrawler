#coding:utf8
#author:tntC4stl3

'''
本模块用于实现线程池
'''
import Queue
import threading
import time

class Worker(threading.Thread):
	'''工作线程'''
	def __init__(self, threadPool, **kargs):
		threading.Thread.__init__(self)
		self.threadPool = threadPool
		self.setDaemon(True)
		self.start()

	def run(self):
		while True:
			try:
				func, args, kargs = self.threadPool.workQueue.get()
				res = func(*args, **kargs)
				self.threadPool.resultQueue.put(res)
				self.threadPool.workDone()
			except Queue.Empty:
				continue
			except Exception, e:
				print 'worker error:', e
		
class ThreadPool(object):
	'''线程池类'''
	def __init__(self, threadNum):
		# 线程数
		self.threadNum = threadNum
		# 工作队列
		self.workQueue = Queue.Queue()
		# 结果队列
		self.resultQueue = Queue.Queue()
		# 线程池
		self.threadPool = []

	def startThreads(self):
		'''启动线程'''
		for i in range(self.threadNum):
			self.threadPool.append(Worker(self))

	def workJoin(self, *args, **kargs):
		'''阻塞直到Queue中的所有任务执行结束'''
		self.workQueue.join()

	def workDone(self, *args, **kargs):
		'''任务完成'''
		self.workQueue.task_done()
	
	def addJob(self, func, *args, **kargs):
		'''添加工作任务'''
		self.workQueue.put((func, args, kargs))

	def getResult(self, *args, **kargs):
		return self.resultQueue.get(*args, **kargs)