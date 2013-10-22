#coding:utf8
#author:tntC4stl3

import argparse
import sqlite3
import logging
import requests
import threading
from bs4 import BeautifulSoup
from threadPool import ThreadPool
from Queue import Queue

class Crawler(object):
	"""Main part, carwl the site"""
	def __init__(self, args):
		# 抓取深度
		self.max_deepth = args['deepth']
		# 指定当前深度
		self.current_deepth = 1
		# 线程管理
		self.threadPool = ThreadPool(args['threads'])
		# 指定存取数据库文件
		self.dbfile = args['dbfile']
		# 指定关键字
		self.keyword = args['keyword']
		# 是否自测
		self.testself = args['testself']
		# 当前层待访问的链接，用集合来去重
		self.unvisitedUrl = set()
		self.unvisitedUrl.add(args['url'])
		# 已访问的链接
		self.visitedUrl = set()
		self.q = Queue()
		# http header
		self.header = {
			'Accetpt': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
			'Accetpt-Encoding': 'gzip,deflate,sdch',
			'Connection': 'keep-alive',
			'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.76 Safari/537.36'
		}
		# 连接数据库
		self.connDB()

		self.isRunning = True

	def start(self):
		self.threadPool.startThreads()
		# 判断当前深度
		while self.current_deepth <= self.max_deepth:
			self.taskQueue()
			while not self.q.empty():
				url = self.q.get()
				# 往线程池中添加任务
				self.threadPool.addJob(self.getLinks, url)
			self.threadPool.workJoin() # 等待所有线程完成
			self.current_deepth += 1
		# 爬取结束
		self.isRunning = False
		self.closeDB()

	def fetchPage(self, url, retry=3):
		'''获取页面内容'''
		try:
			self.r = requests.get(url, headers=self.header, timeout=3)
			if self.r.status_code == requests.codes.ok:
				source = self.r.text
				self.writeDB(url, source)
				return source
		except Exception, e:
			if retry>0:
				return self.fetchPage(url, retry-1)
			else:
				logging.error('Open failed for 3 time: %s' % url)

	def getLinks(self, url):
		'''从页面源代码获取所有链接'''
		source = self.fetchPage(url)
		if not source:
			return
		try:
			soup = BeautifulSoup(source)
		except Exception, e:
			log.error('Get hrefs faild: %s' % url)
			return
		a_tags = soup.find_all('a', href=True)
		for a_tag in a_tags:
			link = a_tag.get('href')
			# TODO：未处理相对路径的情况
			# 如果链接存在于已访问集合或者待访问集合，跳过；否则加入待访问集合
			if link in self.visitedUrl or link in self.unvisitedUrl:
				pass
			else:
				self.unvisitedUrl.add(link)

	def rate(self):
		'''获取任务进度'''
		self.unvisitedNum = self.threadPool.workQueue.qsize()
		self.vistedNum = len(self.visitedUrl) - self.unvisitedNum

	def taskQueue(self):
		'''添加任务队列'''
		while self.unvisitedUrl:
			url = self.unvisitedUrl.pop()
			self.q.put(url)
			self.visitedUrl.add(url)

	def connDB(self):
		'''连接数据库'''
		try:
			self.conn = sqlite3.connect(self.dbfile, isolation_level=None, check_same_thread = False) # sqlite3 默认不允许多线程commit
			self.conn.execute("DROP TABLE IF EXISTS pages;")
			self.conn.execute('''CREATE TABLE pages
								(id INTEGER PRIMARY KEY AUTOINCREMENT,
								 url TEXT,
								 source TEXT);''')
			logging.info('Connect db file succeed.')
		except Exception, e:
			logging.critical('Conncet db failed.')
			self.conn = None

	def writeDB(self, url, source):
		'''将抓取到的数据写入数据库'''
		try:
			self.conn.execute("INSERT INTO pages(url, source) VALUES (?, ?)", (url, source))
		except Exception, e:
			logging.error('Commit error: %s' %e)

	def closeDB(self):
		try:
			self.conn.close()
		except Exception, e:
			logging.error('Close database error: %s' %e)		

class ProgressRate(threading.Thread):
	"""用于打印进度信息"""
	def __init__(self, crawler):
		threading.Thread.__init__(self)
		self.name = "Progress Rate"
		self.crawler = crawler

	def run(self):
		import time
		from datetime import datetime
		start = datetime.now()
		print 'Starting Crawl at:', time.ctime()
		while 1:
			if not self.crawler.isRunning:
				break
			self.crawler.rate()
			print '------------------------------'
			print '* Current deepth: %d' % self.crawler.current_deepth
			print '* Already crawled %d' % self.crawler.vistedNum
			print '* Remained        %d' % self.crawler.unvisitedNum
			print '------------------------------'
			time.sleep(10)
		print 'End at:', time.ctime()
		end = datetime.now()
		print 'Spend time: %s' % (end - start)

def logLevel(n):
	levels = {
		1 : logging.CRITICAL,
		2 : logging.ERROR,
		3 : logging.WARNING,
		4 : logging.INFO,
		5 : logging.DEBUG
	}
	try:
		return levels[n]
	except Exception, e:
		return levels[2]
		
def get_parser():
	parser = argparse.ArgumentParser(description='A simple web crawler')
	parser.add_argument('-u', metavar='URL', dest='url', type=str, 
						help='specify the url start to crawl')
	parser.add_argument('-d', metavar='DEEPTH', dest='deepth', default=3, type=int,
						help='specify the max deep')
	parser.add_argument('--threads', default=10, type=int, 
						help='specify the number of threads (default: 10)')
	parser.add_argument('--dbfile', default='data.db', type=str,
						help='sqlite file to store data')
	parser.add_argument('--key', metavar='KEYWORD', dest='keyword', type=str, 
						help='keyword in page (optional)')
	parser.add_argument('--logfile', default='logging.log', dest='logfile', type=str, 
						help='the path to store log file')
	parser.add_argument('-l', metavar='LOG_LEVEL', default='2', dest='level', type=int, 
						help='the detail level of logfile, the larger, the more detail (optional)')
	parser.add_argument('--testself', action='store_true', 
						help='test by self (optional)')
	parser.add_argument('--version', action='version', version='%(prog)s 1.0')
	return parser

def main():
	parser = get_parser()
	# If you prefer to have dict-like view of the attributes, you can use the standard Python idiom, vars() -- from python document
	args = vars(parser.parse_args())
	if not args['url']:
		parser.print_help()
		return 
	if not args['url'].startswith('http'):
		args['url'] = 'http://' + args['url']
	level = logLevel(args['level'])
	logging.basicConfig(filename = args['logfile'], level = level, filemode = 'w', format = '%(asctime)s - %(levelname)s: %(message)s')  
	crawler = Crawler(args)
	rate = ProgressRate(crawler)
	rate.start()
	crawler.start()
		
if __name__ == '__main__':
	main()