#/usr/bin/python
#coding=utf-8

import os
import sys
import MySQLdb

class Mysql(object):
    def __init__(self, username, passwd, dbname, host, port = 3306, charset = 'utf8'):
        self.user = username
        self.passwd = passwd
        self.dbname = dbname
        self.host = host
        self.port = int(3306)
        self.charset = charset
        self.getConn()

    def getConn(self):
        try:
            self.conn = MySQLdb.connect(host = self.host, 
                user = self.user,
                passwd = self.passwd,
                db = self.dbname,
                port = self.port,
                charset = self.charset)

            self.cursor = self.conn.cursor()
        except Exception as e:
            print(e)
            self.disConn
            os._exit(0)

    def disConn(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()
            self.conn = None

    def execute(self, sql, commit = False):
        try:
            ret = self.cursor.execute(sql)
            if commit:
                self.conn.commit()
            return ret
        except Exception as e:
            print sys._getframe().f_lineno
            print(e)
            self.conn.rollback()
            return -1

    def executemany(self, sql, args, commit = False):
        try:
            ret = self.cursor.executemany(sql, args)
            if commit:
                self.conn.commit()
            return ret
        except Exception as e:
            print sys._getframe().f_lineno
            print(e)
            self.conn.rollback()
            return -1

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except Exception as e:
            return -1

if __name__ == '__main__':
    mysqldb = Mysql('root', '123456', 'connector', '172.7.102.215')
    params = [(i, "myphoto", "http://baidu.pan.com/%d.jpg" % i, "photo-%d" % i, "2017-06-01 20:25:23") for i in range(6000)]
    sql = "insert into PSIS_CZRKZP values (%s, %s, %s, %s, %s)"
    mysqldb.executemany(sql, params, True)
"""
    sql = "select * from PSIS_CZRKZP"
    mysqldb.execute(sql)
    results = mysqldb.fetchall()
    for row in results:
        idx = row[0]
        xp = row[1]
        xpurl = row[2]
        xpid = row[3]
        zhgxsj = row[4]
        print "%d | %s | %s | %s | %s" % (idx, xp, xpurl, xpid, zhgxsj)
"""
