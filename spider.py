#!/usr/bin/env python
#coding:utf8
#author:tntC4stl3

import argparse
import sqlite3
import logging

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
		# 待访问的链接，用集合来去重
		self.unvisitedUrl = set()
		# 已访问的链接
		self.visitedUrl = set()

	def start(self):
		pass

	# 连接数据库
	def connDB(self):
		try:
			self.conn = sqlite3.connect(self.dbfile)
			self.conn.execute("DROP TABLE IF EXIST pages;")
			self.conn.execute('''CREATE TABLE pages
								(id INTEGER PRIMARY KEY AUTOINCREMENT,
								 url TEXT,
								 source TEXT)''')
			self.conn.commit()
		except Exception, e:
			self.conn = None

	# 将抓取到的数据写入数据库
	def writeDB(self, url, source):
		try:
			self.conn.execute("INSERT INTO pages(url, source) VALUES (?, ?)", (url, source))
			self.conn.commit()
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
	parser.add_argument('-d', metavar='DEEPTH', dest='deepth', default=3, 
						help='specify the max deep')
	parser.add_argument('--thread', default=10, type=int, 
						help='specify the number of threads (default: 10)')
	parser.add_argument('--dbfile', default='data.db', 
						help='sqlite file to store data')
	parser.add_argument('--key', metavar='KEYWORD', dest='keyword', type=str, 
						help='keyword in page (optional)')
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
		
if __name__ == '__main__':
	command_line_runner()