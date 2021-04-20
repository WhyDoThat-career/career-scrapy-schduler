import scrapy
from crawler.items import CrawlerItem
from crawler.data_controller import remove_blank_all,wave_split,arr2str,control_deadline_programmers,remove_xa0
from datetime import datetime
from ML.selfattention import AttentionModel
import time
from crawler import sql_db

class ProgrammersSpider(scrapy.Spider):
    name = 'programmers'
    main_url = 'https://programmers.co.kr'
    start_time = None
    classifier = AttentionModel()
    stop_toggle = False
    last_page_number = 0
    page_number = 1
    
    def table2dict(self,labels,contents) :
        table_dict = dict()
        if '주요 서비스' in labels and remove_blank_all(contents[labels.index('주요 서비스')]) == '' :
            del contents[labels.index('주요 서비스')]
        for index,key in enumerate(labels) :
            if key == '경력' :
                print(contents[index])
                if contents[index] in ['신입','경력 무관'] or contents[index].split(' ~ ')[0] == '0' :
                    table_dict['신입여부'] = 1
                    table_dict['경력여부'] = '무관'
                else :
                    table_dict['신입여부'] = 0
                    table_dict['경력여부'] = arr2str(wave_split(contents[index],'년'))
            elif key == '연봉' :
                table_dict['연봉'] = wave_split(contents[index],'만원')
            else :
                table_dict[key] = contents[index]
        
        if '연봉' not in labels :
            table_dict['연봉'] = None
        if '직무' not in labels :
            table_dict['직무'] = None
        return table_dict
    
    def start_requests(self):
        yield scrapy.Request(url=self.main_url+'/job?job_position%5Bdummy%5D=0&amp;order=recent&amp;_=1610273854107',
                             callback=self.parse_main_page)
        
    def parse_main_page(self, response):
        print('-'*10,'마지막 페이지 번호','-'*10)
        self.last_page_number = int(response.css('#paginate > nav > ul > li:nth-child(8) > a::text').getall()[0])
        print(self.last_page_number)
        print('-'*33)
        #paginate > nav > ul > li:nth-child(8) > a
        yield scrapy.Request(url =self.main_url+f'/job?_=1610273854107&job_position%5Bdummy%5D=0&order=recent&page={self.page_number}',
                            callback=self.parse_number_page)
            # break
    def parse_number_page(self, response) :
        job_card_titles = response.css('#list-positions-wrapper > ul > li > div.item-body > h5 > a::text').getall()
        job_card_companys = response.css('#list-positions-wrapper > ul > li > div.item-body > h6.company-name::text').getall()
        job_card_hrefs = response.css('#list-positions-wrapper > ul > li > div.item-body > h5 > a::attr(href)').getall()
        
        for index,job_card_href in enumerate(job_card_hrefs) :
            check_overlap,result = sql_db.check_data('job_detail',self.main_url+job_card_href)
            if check_overlap :
                if (result['title'] != job_card_titles[index] 
                    or result['company_name'] != job_card_companys[index]):
                    sql_db.insert_center(result.keys(),result.values())
                    sql_db.delete_data('job_detail',result['id'])
                    yield scrapy.Request(url=self.main_url+job_card_href,
                                 callback=self.parse_detail,
                                 meta={'job_card_title':job_card_titles[index],
                                       'job_card_company':remove_blank_all(job_card_companys[index]),
                                       'job_card_href':self.main_url+job_card_href})
                else :
                    self.stop_toggle = True
                    break
            else :
                yield scrapy.Request(url=self.main_url+job_card_href,
                                 callback=self.parse_detail,
                                 meta={'job_card_title':job_card_titles[index],
                                       'job_card_company':remove_blank_all(job_card_companys[index]),
                                       'job_card_href':self.main_url+job_card_href})
        self.page_number += 1
        if self.page_number <= self.last_page_number and not self.stop_toggle:
            yield scrapy.Request(url =self.main_url+f'/job?_=1610273854107&job_position%5Bdummy%5D=0&order=recent&page={self.page_number}',
                            callback=self.parse_number_page)
                            
    def parse_detail(self, response) :
        doc = CrawlerItem()
        print(response.meta['job_card_title'],response.meta['job_card_company'])
        if response.meta['job_card_company'] == '' :
            response.meta['job_card_company'] = response.css('#app > div > div > header > div.header-body.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-8 > h4:nth-child(2)::text').get()

        detail_tag = response.css('#app > div > div > div > div.content-body.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-8 \
                                    > section.section-stacks > table > tbody > tr > td > code::text').getall()
        detail_position = response.css('#app > div > div > div > div.content-body.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-8 \
                                    > section.section-position ').getall()
        detail_requirements = response.css('#app > div > div > div > div.content-body.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-8\
                                    > section.section-requirements ').getall()
        detail_preference = response.css('#app > div > div > div > div.content-body.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-8 \
                                    > section.section-preference ').getall()
        detail_description = response.css('#app > div > div > div > div.content-body.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-8\
                                    > section.section-description ').getall()
        
        
        table_label = response.css('#app > div > div > div > div.content-body.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-8 \
                                    > section.section-summary > table > tbody > tr > td.t-label::text').getall() 
        table_text = response.css('#app > div > div > div > div.content-body.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-8\
                                    > section.section-summary > table > tbody > tr > td.t-content::text').getall()
        
        image = response.css('#app > div > div > div > div.content-side.col-item.col-xs-12.col-sm-12.col-md-12.col-lg-4\
                                > section:nth-child(2) > a > img::attr(src)').get()
        
        table_dict = self.table2dict(table_label,table_text)
        
        # doc 선언
        doc['platform'] = self.name
        
        doc['logo_image'] = image
        doc['title'] = response.meta['job_card_title']
        doc['href'] = response.meta['job_card_href']
        
        doc['main_text'] = remove_xa0(''.join(detail_position+detail_requirements+detail_preference+detail_description).replace("\'",'＇'))
        doc['salary'] = arr2str(table_dict['연봉'])
        doc['skill_tag'] = arr2str(detail_tag).upper()
        doc['sector'] = self.classifier.predict(response.meta['job_card_title'])
        doc['newbie'] = table_dict['신입여부']
        doc['career'] = table_dict['경력여부']
        doc['deadline'] = control_deadline_programmers(table_dict['기간'])
        
        doc['company_name'] = response.meta['job_card_company']
        doc['company_address'] = table_dict['위치']
        doc['crawl_date'] = str(datetime.now())
        doc['big_company'] = 0
        
        yield doc