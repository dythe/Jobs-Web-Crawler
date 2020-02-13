# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from stem import Signal
from stem.control import Controller
import os
import random
from scrapy.conf import settings
import sys
import getpass
import stem.connection
import stem.socket
import urllib2
from logging import log
import logging

def _set_new_ip():

    try:
        controller = Controller.from_port(port=9051)
    except stem.SocketError as exc:
        print 'Unable to connect to port 9051 (%s)' % exc
        sys.exit(1)

    try:
        controller.authenticate(password='tor_password')
        controller.signal(Signal.NEWNYM)
        controller.close()
    except stem.connection.IncorrectSocketType:
        print 'Please check in your torrc that 9051 is the ControlPort.'
        print 'Maybe you configured it to be the ORPort or SocksPort instead?'
        sys.exit(1)

class RandomUserAgentMiddleware(object):
    def process_request(self, request, spider):
        ua  = random.choice(settings.get('USER_AGENT_LIST'))
        if ua:
            request.headers.setdefault('User-Agent', ua)
        #logging.info('User Agent : %s' % ua)

class JobscentralSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        spider.logger.info('Spider exception: %s' % response.url)
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
