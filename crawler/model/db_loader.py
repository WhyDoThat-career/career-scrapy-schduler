import pymysql
import json

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
    
    def load_key(self,key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key
    def conn_mysqldb(self) :
        if not self.MYSQL_CONN.open :
            self.MYSQL_CONN.ping(reconnect=True)
        return self.MYSQL_CONN
    
    def insert_data(self,table,key,data) :
        db = self.conn_mysqldb()
        db_cursor = db.cursor()
        sql_query = f"INSERT INTO {table} ({key})  VALUES ({data})"
        db_cursor.execute(sql_query)
        db.commit()

    def check_data(self,table,href) :
        db = self.conn_mysqldb()
        db_cursor = db.cursor()
        sql_query = f"SELECT * FROM {table} WHERE href = {href}"
        db.cursor.execute(sql_query)
        result = db_cursor.fetchone()
        if not result :
            return False
        else :
            return True

if __name__ == '__main__':

    sql_db = MySQL(key_file='../keys/aws_sql_key.json',
                database='crawl_job')