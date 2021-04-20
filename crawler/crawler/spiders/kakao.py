import scrapy
from crawler.items import CrawlerItem
from crawler.data_controller import remove_blank_all,arr2str,control_deadline_kakao
from datetime import datetime
from ML.selfattention import AttentionModel
from crawler import sql_db

class KakaoSpider(scrapy.Spider):
    name = 'kakao'
    main_url = 'https://careers.kakao.com'
    start_time = None
    classifier = AttentionModel()
    stop_toggle = False
    last_page_number = 0
    page_number = 1

    def start_requests(self):
        yield scrapy.Request(url=self.main_url+'/jobs?page=1',callback=self.parse_main_page)
        
    def parse_main_page(self, response):
        print('-'*10,'마지막 페이지 번호','-'*10)
        self.last_page_number = int(response.css('#mArticle > div > div.paging_list > span > a::text').getall()[-1])
        print(self.last_page_number)
        print('-'*33)
        yield scrapy.Request(url =self.main_url+f'/jobs?page={self.page_number}',callback=self.parse_number_page, dont_filter=True)

    def parse_number_page(self, response) :
        job_card_titles = response.css('#mArticle > div > ul.list_jobs > li > div > div > a > h4::text').getall()
        job_card_companys = response.css('#mArticle > div > ul.list_jobs > li > div > dl:nth-child(2) > dd::text').getall()
        job_card_hrefs = response.css('#mArticle > div > ul.list_jobs > li > div > div > a::attr(href)').getall()
        
        for index,job_card_href in enumerate(job_card_hrefs) :
            job_card_href = job_card_href.split('?')[0]
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
        if self.page_number <= self.last_page_number and not self.stop_toggle :
            yield scrapy.Request(url =self.main_url+f'/jobs?page={self.page_number}',callback=self.parse_number_page, dont_filter=True)

    def parse_detail(self, response) :
        doc = CrawlerItem()
        print(response.meta['job_card_title'],response.meta['job_card_company'])

        detail_tag = response.css('#mArticle > div > div > div.cont_board.board_detail > div > div > span::text').getall()
        detail_main_text = response.css('#mArticle > div > div > div.cont_board.board_detail > div ').getall()
        detail_deadline = response.css('#mArticle > div > div > div.info_board > div.wrap_info > dl > dd:nth-child(6)::text').get()
        
        image = 'https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTljM8p4eQcHSwLAu4bRfA5NdoTtSEQUCSARbZX5YNMRjiA0IJ1vuWCwiIyQKAckRqJwyw&usqp=CAU'
        
        
        # doc 선언
        doc['platform'] = self.name
        
        doc['logo_image'] = image
        doc['title'] = response.meta['job_card_title']
        doc['href'] = response.meta['job_card_href']
        
        doc['main_text'] = ''.join(detail_main_text).replace("\'",'＇')
        doc['salary'] = None
        doc['skill_tag'] = arr2str(detail_tag).upper()
        doc['sector'] = self.classifier.predict(response.meta['job_card_title'])
        doc['newbie'] = 0
        doc['career'] = '2,10'
        doc['deadline'] = control_deadline_kakao(detail_deadline)
        
        doc['company_name'] = response.meta['job_card_company']
        doc['company_address'] = '경기 성남시 분당구 판교역로 235 에이치스퀘어 엔동 (판교)'
        doc['crawl_date'] = str(datetime.now())
        doc['big_company'] = 1
        
        yield doc
