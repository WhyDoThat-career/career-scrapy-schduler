import scrapy
from crawler.items import CrawlerItem
from crawler.data_controller import remove_blank_all,style_image_parse,control_deadline,arr2str
from datetime import datetime,timedelta
from ML.selfattention import AttentionModel,converter
from crawler import sql_db

class WantedSpider(scrapy.Spider):
    name = 'wanted'
    main_url = 'https://www.wanted.co.kr'
    start_time = None
    classifier = AttentionModel()
    stop_toggle = False
    
    def start_requests(self):
        yield scrapy.Request(url=self.main_url+'/wdlist/518?country=kr&job_sort=job.latest_order&years=-1&locations=all',callback=self.parse_main_card)
    
    def parse_main_card(self, response):
        # print(response.text)
        job_card_titles = response.css('#__next > div > div._1yHloYOs_bDD0E-s121Oaa > div._2y4sIVmvSrf6Iy63okz9Qh > div > ul > li > div > a::attr(data-position-name)').getall()
        job_card_companys = response.css('#__next > div > div._1yHloYOs_bDD0E-s121Oaa > div._2y4sIVmvSrf6Iy63okz9Qh > div > ul > li > div > a::attr(data-company-name)').getall()
        job_card_hrefs = response.css('#__next > div > div._1yHloYOs_bDD0E-s121Oaa > div._2y4sIVmvSrf6Iy63okz9Qh > div > ul > li > div > a::attr(href)').getall()

        for index,job_card_href in enumerate(job_card_hrefs) :
            check_overlap,result = sql_db.check_data('job_detail',self.main_url+job_card_href)
            if check_overlap :
                if ((result['title'] != job_card_titles[index] or result['company_name'] != remove_blank_all(job_card_companys[index]))
                    or (datetime.today()-result['crawl_date']) >= timedelta(days=14)) :
                    sql_db.insert_center(result)
                    sql_db.delete_data('job_detail',result['id'])
                    yield scrapy.Request(url=self.main_url+job_card_href,
                                        callback=self.parse_job_detail,
                                        meta={'job_card_title':job_card_titles[index],
                                            'job_card_company':remove_blank_all(job_card_companys[index]),
                                            'job_card_href':self.main_url+job_card_href})
                else :
                    self.stop_toggle = True
                    break
            else :
                yield scrapy.Request(url=self.main_url+job_card_href,
                                        callback=self.parse_job_detail,
                                        meta={'job_card_title':job_card_titles[index],
                                            'job_card_company':remove_blank_all(job_card_companys[index]),
                                            'job_card_href':self.main_url+job_card_href})

    def parse_job_detail(self, response):
        doc = CrawlerItem()
        print(response.meta['job_card_title'],response.meta['job_card_company'])

        detail_main_text = response.css('#__next > div > div._37L2cip40tqu3zm3KC4dAa > div._17tolBMfrAeoPmo6I9pA1P > div._1FVm15xN253istI2zLF_Ax\
                                        > div > div._31EtVNPZ-KwYCXvVZ3927g > section._3_gsSnQyvwrqCAjw47hjWK ').getall()
        detail_deadline = response.css('#__next > div > div._37L2cip40tqu3zm3KC4dAa > div._17tolBMfrAeoPmo6I9pA1P > div._1FVm15xN253istI2zLF_Ax\
                                        > div > div._31EtVNPZ-KwYCXvVZ3927g > section._3XP3DBqOgzsz7P6KrVpbGO > div:nth-child(1) > span.body::text').get()
        detail_addr = response.css('#__next > div > div._37L2cip40tqu3zm3KC4dAa > div._17tolBMfrAeoPmo6I9pA1P > div._1FVm15xN253istI2zLF_Ax\
                                    > div._33u5kCnL62igIXfrIg7Ikl > div._31EtVNPZ-KwYCXvVZ3927g > section._3XP3DBqOgzsz7P6KrVpbGO > div:nth-child(2) > span.body::text').get()
        image = response.css('#__next > div > div._37L2cip40tqu3zm3KC4dAa > div._17tolBMfrAeoPmo6I9pA1P > div._1FVm15xN253istI2zLF_Ax \
                                > div > section._3h_f6TfissC0l7ogPcn7lY > button.left > div.logo::attr(style)').getall()
        
        main_work = response.css('#__next > div > div._37L2cip40tqu3zm3KC4dAa > div._17tolBMfrAeoPmo6I9pA1P > div._1FVm15xN253istI2zLF_Ax\
                                > div._33u5kCnL62igIXfrIg7Ikl > div._31EtVNPZ-KwYCXvVZ3927g > section._3_gsSnQyvwrqCAjw47hjWK\
                                > p:nth-child(3)::text').getall()
        
        require = response.css('#__next > div > div._37L2cip40tqu3zm3KC4dAa > div._17tolBMfrAeoPmo6I9pA1P > div._1FVm15xN253istI2zLF_Ax\
                                > div._33u5kCnL62igIXfrIg7Ikl > div._31EtVNPZ-KwYCXvVZ3927g > section._3_gsSnQyvwrqCAjw47hjWK \
                                > p:nth-child(5) > span::text').getall()
        good = response.css('#__next > div > div._37L2cip40tqu3zm3KC4dAa > div._17tolBMfrAeoPmo6I9pA1P > div._1FVm15xN253istI2zLF_Ax\
                             > div._33u5kCnL62igIXfrIg7Ikl > div._31EtVNPZ-KwYCXvVZ3927g > section._3_gsSnQyvwrqCAjw47hjWK\
                             > p:nth-child(7) > span::text').getall()
        
        sentence = arr2str(main_work+require+good)
        okt_title = converter.pos(sentence)
        sentence = ','.join([tup[0].upper() for tup in okt_title if tup[1] == 'Alpha'])
        sentence = arr2str(set(sentence
                    .replace('JS','JAVASCRIPT')
                    .replace('NODE','NODE.JS')
                    .replace('VUE','VUE.JS')
                    .replace('JAVASCRIPTP','JSP')
                    .replace('NATIVE','REACT-NATIVE')
                    .split(',')
                    ))
        
        
        doc['platform'] = self.name

        doc['logo_image'] = style_image_parse(image)[0]
        doc['title'] = response.meta['job_card_title']
        doc['href'] = response.meta['job_card_href']
        
        doc['main_text'] = ''.join(detail_main_text).replace("\'",'???')
        doc['salary'] = None
        doc['skill_tag'] = sentence
        doc['sector'] = self.classifier.predict(response.meta['job_card_title'])
        doc['newbie'] = 1
        doc['career'] = '??????'
        doc['deadline'] = control_deadline(detail_deadline)
        
        doc['company_name'] = response.meta['job_card_company']
        doc['company_address'] = detail_addr
        doc['crawl_date'] = str(datetime.now())
        doc['big_company'] = 0
        
        yield doc