import sys

import pymysql

from conf import config


class MysqlAction(object):

    def __init__(self):
        self.db = pymysql.connect(config.HOST, config.USERNAME, config.PASSWORD, config.DB)
        self.cursor = self.db.cursor()
        print('启动数据库')


    def close_db(self):
        print('关闭数据库')
        self.db.close()

    def execute_sql_return_result(self, sql):
        """查询语句 执行函数"""
        result = 0
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            self.db.commit()
        except:
            print(sys.exc_info()[0], sys.exc_info()[1])
            self.db.rollback()
        return result

    def execute_sql_only(self, sql):
        """增删改 语句 执行函数"""
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except:
            print('sql错误')
            print(sys.exc_info()[0], sys.exc_info()[1])
            self.db.rollback()