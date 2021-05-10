import pymysql
import json
import pandas
from pymysql.cursors import DictCursor
from crawler.data_controller import arr2str

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
                                passwd = 'root',
                                db = 'career-center',
                                charset='utf8mb4'
                                )
    def create_sql_item(self,item,dtype) :
        if dtype == 'string' or 'datetime':
            return "'{}'".format(item)
        elif dtype == 'bool' or 'int':
            return "{}".format(item)
    def dict_list2string(self,items,dtype_map) :
        key_arr = []
        item_arr = []
        for key, item in items.items() :
            if key != 'id' and item != None :
                key_arr.append(key)
                item_arr.append(self.create_sql_item(item,dtype=dtype_map[key]))
        return arr2str(key_arr),arr2str(item_arr)
    
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
    
    def insert_data(self,table,items,dtype_map) :
        skey,sdata = self.dict_list2string(items,dtype_map)
        db = self.conn_mysqldb()
        db_cursor = db.cursor()
        sql_query = f"INSERT INTO {table} ({skey}) VALUES ({sdata})"
        db_cursor.execute(sql_query)
        db.commit()
        
    def insert_center(self,items) :
        with open('model/dtype_map.json') as file :
            dtype_map = json.load(file)
        skey,sdata = self.dict_list2string(items,dtype_map)
        db = self.conn_data_center()
        db_cursor = db.cursor()
        sql_query = f"INSERT INTO regacy_job_detail({skey}) VALUES ({sdata})"
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
        sql_query = f"SELECT * FROM {table} WHERE href = \'{href}\'"
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