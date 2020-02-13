# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors
import codecs
import json
from logging import log
import logging
from datetime import datetime, date, time, timedelta
import time
import json
from contextlib import closing

class JobscentralPipeline(object):
    
    conn = None
    msg = ''

    def __init__(self,dbpool):
        self.dbpool=dbpool  #try to remove dbpool redundant code
        #self.connect()

    @classmethod
    def from_settings(cls,settings):
        dbparams=dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=False,
        )
        dbpool=adbapi.ConnectionPool('MySQLdb',**dbparams)
        return cls(dbpool)

    def process_item(self, item, spider):
        query=self.dbpool.runInteraction(self._conditional_insert,item)
        query.addErrback(self._handle_error,item,spider)
        return item

    def connect(self):
        conn = MySQLdb.connect(host='127.0.0.1',
                   user='root',
                   passwd='',
                   db='jobs',
                   charset='utf8'
                    )
        # self.conn = MySQLdb.connect(host='192.168.1.130',
        #            user='desktop',
        #            passwd='',
        #            db='jobs',
        #            charset='utf8'
        #            )

        return conn
    
    def checkRecord(self, query, params, msg):

        with closing(self.connect()) as my_conn:
            with closing(my_conn.cursor()) as my_curs:
                try:
                    my_curs.execute(query, params)
                    fetch = my_curs.fetchone()
                    number_of_rows=fetch[0]
                    return number_of_rows
                except (MySQLdb.OperationalError):
                    logging.info("Attempting to reconnect for select query") 
                #     self.connect()
                #     self.checkRecord(query, params)
                except mysql.connector.Error:
                    logging.info("ERROR!")
        logging.info(msg)

    def checkCategory(self, query, params, msg):

        with closing(self.connect()) as my_conn:
            with closing(my_conn.cursor()) as my_curs:
                try:
                    my_curs.execute(query, params)
                    fetch = my_curs.fetchone()

                    if fetch:
                        return fetch[0]
                    else:
                        return
                except (MySQLdb.OperationalError):
                    logging.info("Attempting to reconnect for select query") 
                #     self.connect()
                #     self.checkRecord(query, params)
                except mysql.connector.Error:
                    logging.info("ERROR!")
        logging.info(msg)

    def insertRecord(self, query, params, msg):

        with closing(self.connect()) as my_conn:
            logging.info("MYCONN" + str(my_conn))
            with closing(my_conn.cursor()) as my_curs:
                try:
                    logging.info("EXECUTE!")
                    my_curs.execute(query, params)
                except (MySQLdb.OperationalError):
                    logging.info("Attempting to reconnect for insert query") 
                #     self.connect()
                #     self.checkRecord(query, params)
                except mysql.connector.Error:
                    logging.info("ERROR!")
                finally:
                    my_conn.commit()

        logging.info(msg)

    def updateCategory(self, query, params, msg):

        with closing(self.connect()) as my_conn:
            with closing(my_conn.cursor()) as my_curs:
                try:
                    my_curs.execute(query, params)
                except (MySQLdb.OperationalError):
                    logging.info("Attempting to reconnect for update query") 
                #     self.connect()
                #     self.checkRecord(query, params)
                except mysql.connector.Error:
                    logging.info("ERROR!")
                finally:
                    my_conn.commit()

        logging.info(msg)

    def _conditional_insert(self,tx,item):
        
        newItem = None
        catfetch = None
        fetch = None

        if item['company_name'] is None:
            item['company_name'] = ''
        else:
            s = item['company_name'] 
            s = s.replace("'", "\\'")
            item['company_name'] = s
 
        #selectquery = "SELECT count(*) FROM dbJobs WHERE jobdetailsurl LIKE '" + item['jobdetailsurl'] + "' AND company_name LIKE '" + item['company_name'] + "' AND title LIKE '" + item['title'] + "' AND timestamp LIKE '" + str(item['timestamp']) + "'"
        #selectquery = 'SELECT DATE_FORMAT(timestamp, "%d-%b-%y") AS timestamp FROM dbJobs WHERE jobdetailsurl LIKE "' + item['jobdetailsurl'] + '" AND company_name LIKE "' + item['company_name'] + '" AND title LIKE "' + item['title'] + '"'
    
        #logging.info("Select QUERY: %s", selectquery)
        
        selectquery = "SELECT count(*) FROM dbJobs WHERE jobdetailsurl LIKE %s AND company_name LIKE %s AND title LIKE %s AND timestamp = %s"
        selectparams = (item['jobdetailsurl'], item['company_name'], item['title'], item['timestamp'])
        msg = "Checking for whether record exists in Database"

        fetch = self.checkRecord(selectquery, selectparams, msg)

        logging.info("Fetch value is %s", fetch)

        if fetch >= 1:
            logging.info("Record exists, not inserting current record into database")
            newItem = 1
        elif fetch == 0:
            insertquery = "INSERT INTO dbJobs(title, company_name, jobdetailsurl, position, jobnature, qualification, job_description, timestamp, category, source, subcategory) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            insertparams = (item["title"],item["company_name"],item["jobdetailsurl"],item["position"],item["jobnature"],item["qualification"],item["job_description"],item["timestamp"],item["category"],item["source"],item["subcategory"])
            msg = "Database insertion successful"
            self.insertRecord(insertquery, insertparams, msg)


        ######## PROCEED WITH CATEGORY ############
        if newItem is not None:
            categoryquery = "SELECT category FROM dbJobs WHERE jobdetailsurl LIKE %s AND company_name LIKE %s AND title LIKE %s AND timestamp = %s"
            categoryparams = (item['jobdetailsurl'], item['company_name'], item['title'], item['timestamp'])
            msg = "Checking whether category exists in current record"

            catfetch = self.checkCategory(categoryquery, categoryparams, msg)

            logging.info("Catfetch value is %s", catfetch)
            #logging.info("Catfetch[0] value is %s", catfetch[0])

            if catfetch is not None:
                #catfetch2 = catfetch[0]
                appendvalue = item['category']
                #logging.info("catfetch2: %s", catfetch2)
                my_list = catfetch.split(",")

                if not appendvalue in my_list:
                    logging.info("Appending value %s to list", appendvalue)
                    my_list.append(appendvalue)
                    logging.info("Categories as a list before sorting: %s", my_list)

                    my_list.sort()
                    logging.info("Categories as a list after sorting: %s", my_list)

                    str_my_list = ','.join(str(e) for e in my_list)
                    logging.info("Categories as a string: %s", str_my_list)

                    updatecatquery = "UPDATE dbJobs SET category=%s WHERE jobdetailsurl LIKE %s AND company_name LIKE %s AND title LIKE %s AND timestamp = %s"
                    updatecatparams = (str_my_list, item['jobdetailsurl'], item['company_name'], item['title'], item['timestamp'])
                    msg = "Database update successful with categories " + str_my_list
                    self.updateCategory(updatecatquery, updatecatparams, msg)
    
                else:
                    logging.info("Appending of value %s not needed as category already exists", appendvalue)
                        
            
    def _handle_error(self, failure, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print item
        print failure

    def close_spider(self, spider):
        """ Cleanup function, called after crawing has finished to close open
            objects.
            Close ConnectionPool. """
        spider.log('Closing connection pool...')
        self.dbpool.close()
        #self.conn.close()