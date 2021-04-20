import scrapy
from scrapy.selector import Selector
from crawler.items import CrawlerItem
from crawler.data_controller import wave_split,arr2str
from datetime import datetime
from ML.selfattention import AttentionModel
from crawler import sql_db

class RoketpunchSpider(scrapy.Spider):
    name = 'roketpunch'
    main_url = 'https://www.rocketpunch.com'
    start_time = None
    classifier = AttentionModel()
    stop_toggle = False
    last_page_number = 0
    page_number = 1

    def table2dict(self,labels,contents) :
        table_dict = dict()
        for index,key in enumerate(labels) :
            if key == '경력 여부' :
                if '신입' in contents[index] :
                    table_dict['신입여부'] = 1
                    table_dict['경력여부'] = '무관'
                else :
                    table_dict['신입여부'] = 0
                    table_dict['경력여부'] = '2,10'
            elif key == '연봉' : 
                table_dict['연봉'] = arr2str(wave_split(contents[index],'만원',wave_type='-'))
                if table_dict['연봉'] == '채용담당자에게문의' :
                    table_dict['연봉'] = None
            else :
                table_dict[key] = contents[index]
        return table_dict
    

    def start_requests(self):
        yield scrapy.Request(url=self.main_url+'/jobs?job=1&page=1/',callback=self.parse_main_page)
    
    def parse_main_page(self, response):
        # print(response.text)
        print('-'*10,'크롤링 결과','-'*10)
        self.last_page_number = int(response.css('#search-results > div.ui.blank.right.floated.segment > div > div.tablet.computer.large.screen.widescreen.only > a:nth-child(7)::text').getall()[0])
        print(self.last_page_number)
        print('-'*33)
        yield scrapy.Request(url =self.main_url+f'/jobs?job=1&page={self.page_number}',callback=self.parse_number_page)

    def parse_number_page(self, response) :
        job_cards = response.css('#company-list > div > div.content').getall()
        images = response.css('#company-list > div > div.logo.image').getall()
        for index,job_card in enumerate(job_cards) :
            job_card_company = Selector(text=job_card).css('div.company-name > a > h4 > strong::text').get()
            job_card_titles = Selector(text=job_card).css('div.company-jobs-detail > div > div > a.nowrap.job-title.primary.link::text').getall()
            job_card_hrefs = Selector(text=job_card).css('div.company-jobs-detail > div > div > a.nowrap.job-title.primary.link::attr(href)').getall()
            image = Selector(text=images[index]).css('a > div > img::attr(src)').get()
            
            for index,job_card_href in enumerate(job_card_hrefs) :
                check_overlap,result = sql_db.check_data('job_detail',self.main_url+job_card_href)
                if check_overlap :
                    self.stop_toggle = True
                    break
                else :
                    yield scrapy.Request(url=self.main_url+job_card_href,
                                        callback=self.parse_job_detail,
                                        meta={'job_card_title':job_card_titles[index],
                                                'job_card_company':job_card_company,
                                                'job_card_href':self.main_url+job_card_href,
                                                'logo_image' : image})
        self.page_number += 1
        if self.page_number <= self.last_page_number and not self.stop_toggle :
            yield scrapy.Request(url =self.main_url+f'/jobs?job=1&page={self.page_number}',callback=self.parse_number_page)

            
    def parse_job_detail(self, response) :
        doc = CrawlerItem()
        print(response.meta['job_card_title'],response.meta['job_card_company'])
        detail_tag = response.css('#wrap > div.eight.wide.job-content.column > section > div.job-specialties > a::text').getall()
        detail_main_work = response.css('#wrap > div.eight.wide.job-content.column > section:nth-child(1) ').getall()
        detail_foreign = response.css('#wrap > div.eight.wide.job-content.column > section:nth-child(6) ').getall()
        detail_require = response.css('#wrap > div.eight.wide.job-content.column > section:nth-child(8) ').getall()
        detail_welfare = response.css('#wrap > div.eight.wide.job-content.column > section:nth-child(16) ').getall()
        detail_addr = response.css('#wrap > div.eight.wide.job-content.column > section > div.office.item > span.address::text').get()
        
        table_label = response.css('#wrap > div.four.wide.job-infoset.column > div > div:nth-child(3) > div > div > div.title::text').getall()
        table_text = response.css('#wrap > div.four.wide.job-infoset.column > div > div:nth-child(3) > div > div > div.content::text').getall()
        
        table_dict = self.table2dict(table_label,table_text)
        if table_dict['마감일'] == '\n                ':
            deadline = response.css('#wrap > div.four.wide.job-infoset.column > div > div:nth-child(3) > div > div > div.content > strong::text').get()
        else :
            deadline = None
        
        doc['platform'] = self.name

        doc['logo_image'] = response.meta['logo_image']
        doc['title'] = response.meta['job_card_title']
        doc['href'] = response.meta['job_card_href']
        
        doc['main_text'] = '<br>'.join(detail_main_work+detail_foreign+detail_require+detail_welfare).replace("\'",'＇')
        doc['salary'] = table_dict['연봉']
        doc['skill_tag'] = arr2str(detail_tag).upper()
        doc['sector'] = self.classifier.predict(response.meta['job_card_title'])
        doc['newbie'] = table_dict['신입여부']
        doc['career'] = table_dict['경력여부']
        doc['deadline'] = deadline
        
        doc['company_name'] = response.meta['job_card_company']
        doc['company_address'] = detail_addr
        doc['crawl_date'] = str(datetime.now())
        doc['big_company'] = 0
        
        yield doc