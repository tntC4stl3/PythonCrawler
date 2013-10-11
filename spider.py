#!/usr/bin/env python
#coding:utf8
#author:tntC4stl3

import argparse
import sqlite3
import logging
import requests
from bs4 import BeautifulSoup
from Queue import Queue

class Crawler(object):
	"""Main part, carwl the site"""
	def __init__(self, args):
		# 初始链接
		self.root = args['url']
		# 抓取深度
		self.max_deepth = args['deepth']
		# 指定当前深度
		self.current_deepth = 1
		# 指定线程数
		self.theadnum = args['threads']
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
		# http header
		self.header = {
			'Accetpt': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
			'Accetpt-Encoding': 'gzip,deflate,sdch',
			'Connection': 'keep-alive',
			'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.76 Safari/537.36'
		}
		self.q = Queue()
		# 连接数据库
		self.connDB()

	def start(self):
		while self.current_deepth <= self.max_deepth:
			print "current deepeh: %d" % self.current_deepth
			self._taskQueue()
			while not self.q.empty():
				url = self.q.get()
				self._getLinks(url)
			self.current_deepth += 1
		self.closeDB()

	def _fetchPage(self, url, retry=3):
		'''获取页面内容'''
		try:
			r = requests.get(url, headers=self.header, timeout=10)
			if r.status_code == requests.codes.ok:
				source = r.text
				self.writeDB(url, source)
				return source
			else:
				pass
				# log.warning('[WARNING][Open failed][status code:%d URL:%s]' % (r.status_code, self.url)
		except Exception, e:
			print e
			if retry>0:
				return self._fetchPage(url, retry-1)

	def _getLinks(self, url):
		'''从页面源代码获取所有链接'''
		source = self._fetchPage(url=url)
		if not source:
			self.visitedUrl.add(url)
			return
		try:
			soup = BeautifulSoup(source)
		except Exception, e:
			print e
				#log.warning('[WARNING][Get href faild][URL:%s]' % self.url)
			return
		a_tags = soup.find_all('a', href=True)
		for a_tag in a_tags:
			link = a_tag.get('href')
			if link in self.visitedUrl or link in self.unvisitedUrl:
				pass
			else:
				self.unvisitedUrl.add(link)

	def _taskQueue(self):
		'''添加任务队列'''
		while self.unvisitedUrl:
			url = self.unvisitedUrl.pop()
			self.q.put(url)
			self.visitedUrl.add(url)

	def connDB(self):
		'''连接数据库'''
		try:
			self.conn = sqlite3.connect(self.dbfile)
			self.conn.execute("DROP TABLE IF EXISTS pages;")
			self.conn.execute('''CREATE TABLE pages
								(id INTEGER PRIMARY KEY AUTOINCREMENT,
								 url TEXT,
								 source TEXT);''')
			self.conn.commit()
			print 'Connect db success.'
		except Exception, e:
			logging.info('Conncet db Failed.')
			self.conn = None

	def writeDB(self, url, source):
		'''将抓取到的数据写入数据库'''
		try:
			self.conn.execute("INSERT INTO pages(url, source) VALUES (?, ?)", (url, source))
			self.conn.commit()
			print "insert success."
		except Exception, e:
			print "Commit error: %s" % e

	def closeDB(self):
		try:
			self.conn.close()
		except Exception, e:
			print "Close database error: %s" % e


def get_parser():
	parser = argparse.ArgumentParser(description='A simple web crawler')
	parser.add_argument('-u', metavar='URL', dest='url', type=str, 
						help='specify the url start to crawl')
	parser.add_argument('-d', metavar='DEEPTH', dest='deepth', default=3, type=int,
						help='specify the max deep')
	parser.add_argument('--threads', default=10, type=int, 
						help='specify the number of threads (default: 10)')
	parser.add_argument('--dbfile', default='data.db', type=str
						help='sqlite file to store data')
	parser.add_argument('--key', metavar='KEYWORD', dest='keyword', type=str, 
						help='keyword in page (optional)')
	parser.add_argument('--logfile', default='logging.log', dest='logfile', type=str, 
						help='the path to store log file')
	parser.add_argument('-l', metavar='LOG_LEVEL', dest='level', type=int, 
						help='the detail level of logfile, the larger, the more detail (optional)')
	parser.add_argument('--testself', action='store_true', 
						help='test by self (optional)')
	parser.add_argument('--version', action='version', version='%(prog)s 1.0')
	return parser


def command_line_runner():
	parser = get_parser()
	# If you prefer to have dict-like view of the attributes, you can use the standard Python idiom, vars() -- from python document
	args = vars(parser.parse_args())
	if not args['url']:
		parser.print_help()
		return 
	if not args['url'].startswith('http'):
		args['url'] = 'http://' + args['url']
	crawler = Crawler(args)
	crawler.start()
		
if __name__ == '__main__':
	command_line_runner()