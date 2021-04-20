import scrapy
from crawler.items import CrawlerItem
from crawler.data_controller import style_image_parse,control_deadline_naver,control_skill_tag_naver
from datetime import datetime
from ML.selfattention import AttentionModel
from crawler import sql_db

class NaverSpider(scrapy.Spider):
    name = 'naver'
    main_url = 'https://recruit.navercorp.com'
    start_time = None
    classifier = AttentionModel()
    stop_toggle = False

    def recognize_newbie(self,title) :
        if '신입' in title or '인턴' in title :
            ret_dict = {'신입여부' : 1 , '경력여부' : '무관'}
        else :
            ret_dict = {'신입여부' : 0 , '경력여부' : '2,10'}
        
        return ret_dict
    
    def recognize_company(self,text) :
        if text in [None,'',' ']:
            return '네이버'
        else :
            return text

    def start_requests(self):
        yield scrapy.Request(url=self.main_url+'/naver/job/list/developer',callback=self.parse_main_card)
    
    def parse_main_card(self, response):
        # print(response.text)
        job_card_titles = response.css('#jobListDiv > ul > li > a > span > strong::text').getall()
        logo_image = response.css('#jobListDiv > ul > li > a > span > span.crd_ci > img::attr(src)').getall()
        job_card_hrefs = response.css('#jobListDiv > ul > li > a::attr(href)').getall()
        detail_deadlines = response.css('#jobListDiv > ul > li > a > span > em::text').getall()
        print(len(job_card_titles),len(logo_image),len(job_card_hrefs),len(detail_deadlines))

        for index,detail_deadline in enumerate(detail_deadlines) :
            check_overlap,result = sql_db.check_data('job_detail',self.main_url+job_card_hrefs[index])
            if check_overlap :
                if (result['title'] != job_card_titles[index]):
                    sql_db.insert_center(result.keys(),result.values())
                    sql_db.delete_data('job_detail',result['id'])
                    yield scrapy.Request(url=self.main_url+job_card_hrefs[index],
                                    callback=self.parse_job_detail,
                                    meta={'job_card_title':job_card_titles[index],
                                        'logo_image':self.main_url+'/'+logo_image[index],
                                        'job_card_href':self.main_url+job_card_hrefs[index],
                                        'detail_deadline':detail_deadline})
                else :
                    self.stop_toggle = True
                    break
            else :
                yield scrapy.Request(url=self.main_url+job_card_hrefs[index],
                                    callback=self.parse_job_detail,
                                    meta={'job_card_title':job_card_titles[index],
                                        'logo_image':self.main_url+'/'+logo_image[index],
                                        'job_card_href':self.main_url+job_card_hrefs[index],
                                        'detail_deadline':detail_deadline})

    def parse_job_detail(self, response):
        doc = CrawlerItem()
        print(response.meta['job_card_title'],response.meta['logo_image'])

        job_card_companys = response.css('#content > div > div.career_detail > div.dtl_context > div.context_area > div.n_career_header > h1 > span > font::text').get()
        detail_main_text = response.css('#content > div > div.career_detail > div.dtl_context > div.context_area ').getall()
        detail_tag = response.css('#content > div > div.career_detail > div.dtl_context > span > a::text').getall()

        newbie_dict = self.recognize_newbie(response.meta['job_card_title'])

        doc['platform'] = self.name

        doc['logo_image'] = response.meta['logo_image']
        doc['title'] = response.meta['job_card_title']
        doc['href'] = response.meta['job_card_href']
        
        doc['main_text'] = ''.join(detail_main_text).replace("\'",'＇')
        doc['salary'] = None
        doc['skill_tag'] = control_skill_tag_naver(detail_tag)
        doc['sector'] = self.classifier.predict(response.meta['job_card_title'])
        doc['newbie'] = newbie_dict['신입여부']
        doc['career'] = newbie_dict['경력여부']
        doc['deadline'] = control_deadline_naver(response.meta['detail_deadline'])
        
        doc['company_name'] = self.recognize_company(job_card_companys)
        doc['company_address'] = '경기 성남시 분당구 불정로 6 (판교)'
        doc['crawl_date'] = str(datetime.now())
        doc['big_company'] = 1
        
        yield doc