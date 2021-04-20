import pymysql
import json
import pandas
from pymysql.cursors import DictCursor

class MySQL :
    def __init__(self,key_file,database) :
        KEY = self.load_key(key_file)
        self.MYSQL_HOST = KEY['host']
        self.MYSQL_CONN = pymysql.connect(
                                host = self.MYSQL_HOST,
                                user=KEY['user'],
                                passwd=KEY['password'],
                                db=database,
                                charset='utf8mb4')
        self.data_center = pymysql.connect(
                                host = '15.164.140.140',
                                user = KEY['user'],
                                passwd = KEY['password'],
                                db = 'career-center',
                                charset='utf8mb4'
                                )
    
    def load_key(self,key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key
    def conn_mysqldb(self) :
        if not self.MYSQL_CONN.open :
            self.MYSQL_CONN.ping(reconnect=True)
        return self.MYSQL_CONN
    def conn_data_center(self) :
        if not self.data_center.open :
            self.data_center.ping(reconnect=True)
        return self.data_center
    def insert_data(self,table,key,data) :
        db = self.conn_mysqldb()
        db_cursor = db.cursor()
        sql_query = f"INSERT INTO {table} ({key})  VALUES ({data})"
        db_cursor.execute(sql_query)
        db.commit()
    def insert_center(self,key,data) :
        skey = ','.join(key)
        sdata = ','.join(data)
        db = self.conn_data_center()
        db_cursor = db.cursor()
        sql_query = f"INSERT INTO regacy_job_detail\
                         ({key})  VALUES ({data})"
        db_cursor.execute(sql_query)
        db.commit()

    def delete_data(self,table,id) :
        db = self.conn_mysqldb()
        db_cursor = db.cursor()
        sql_query = f"DELETE FROM {table} WHERE `id` = {id}"
        db_cursor.execute(sql_query)
        db.commit()

    def check_data(self,table,href) :
        db = self.conn_mysqldb()
        db_cursor = db.cursor(DictCursor)
        sql_query = f"SELECT id,title,company_name FROM {table} WHERE href = \'{href}\'"
        db_cursor.execute(sql_query)
        result = db_cursor.fetchone()
        print(result)
        if not result :
            return False,False
        else :
            return True,result
        
    def get_distinct_data(self,table,key) :
        db = self.conn_mysqldb()
        db_cursor = db.cursor()
        sql_query = f"SELECT DISTINCT {key} FROM {table}"
        db_cursor.execute(sql_query)
        result = db_cursor.fetchall()
        return [item[0] for item in result]

if __name__ == '__main__':

    sql_db = MySQL(key_file='../keys/aws_sql_key.json',
                database='crawl_job')