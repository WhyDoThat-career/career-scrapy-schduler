# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import datetime
from crawler.data_controller import arr2str
from model.postman import login_mail,send_mail
from crawler import sql_db

with open('model/dtype_map.json') as file :
    dtype_map = json.load(file)

with open('model/dtype_company.json') as file :
    dtype_company = json.load(file)

class CrawlerPipeline:
    def create_sql_item(self,item,dtype) :
        if dtype == 'string' or 'datetime':
            return "'{}'".format(item)
        elif dtype == 'bool' or 'int':
            return "{}".format(item)
            
    def process_item(self, items, spider):
        print('@'*10,'pipeline','@'*10)
        key_arr = []
        item_arr = []
        # print(items)
        for key, item in items.items() :
            if item != None :
                key_arr.append(key)
                item_arr.append(self.create_sql_item(item,dtype=dtype_map[key]))
        
        sql_db.insert_data('job_detail',arr2str(key_arr),arr2str(item_arr))
        print('[Success]insert data for mysql')
        return items

    def open_spider(self,spider) :
        sql_db.MYSQL_CONN = sql_db.conn_mysqldb()
        spider.start_time = datetime.datetime.now()

    def close_spider(self,spider) :
        email_postman = login_mail()
        stats = spider.crawler.stats.get_stats()
        report = f'''
        크롤러 이름 : {spider.name}
        시작 시간 : {spider.start_time}
        종료 시간 : {datetime.datetime.now()}
        진행 시간 : {datetime.datetime.now()-spider.start_time} 
        크롤링 요청 count : {stats['response_received_count']}
        크롤링 성공 count : {stats['item_scraped_count']}
        크롤링 실패 count : {stats['response_received_count']-stats['item_scraped_count']}
        '''
        send_mail(email_postman,f'{datetime.date.today()} {spider.name} 크롤링 보고서',
                f'Summary stats from {spider.name} crawler:\n\n{report}',
                ['jgy206@gmail.com'])#,'endndnjs2@gmail.com','kth9193@gmail.com'])

class JobPlanetPipeline:
    def create_sql_item(self,item,dtype) :
        if dtype == 'string' or 'datetime':
            return "'{}'".format(item)
        elif dtype == 'bool' or 'int' or 'float':
            return "{}".format(item)
            
    def process_item(self, items, spider):
        print('@'*10,'pipeline','@'*10)
        key_arr = []
        item_arr = []
        # print(items)
        for key, item in items.items() :
            if item != None :
                key_arr.append(key)
                item_arr.append(self.create_sql_item(item,dtype=dtype_company[key]))
        
        sql_db.insert_data('company_info',arr2str(key_arr),arr2str(item_arr))
        print('[Success]insert data for mysql')
        return items

    def open_spider(self,spider) :
        sql_db.MYSQL_CONN = sql_db.conn_mysqldb()
        spider.start_time = datetime.datetime.now()

    def close_spider(self,spider) :
        email_postman = login_mail()
        stats = spider.crawler.stats.get_stats()
        report = f'''
        크롤러 이름 : {spider.name}
        시작 시간 : {spider.start_time}
        종료 시간 : {datetime.datetime.now()}
        진행 시간 : {datetime.datetime.now()-spider.start_time} 
        크롤링 요청 count : {stats['response_received_count']}
        크롤링 성공 count : {stats['item_scraped_count']}
        크롤링 실패 count : {stats['response_received_count']-stats['item_scraped_count']}
        '''
        send_mail(email_postman,f'{datetime.date.today()} {spider.name} 크롤링 보고서',
                f'Summary stats from {spider.name} crawler:\n\n{report}',
                ['jgy206@gmail.com','endndnjs2@gmail.com','kth9193@gmail.com'])