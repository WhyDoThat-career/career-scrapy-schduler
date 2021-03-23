import scrapy
from crawler.items import JobplanetItem
from datetime import datetime


class JobplanetSpider(scrapy.Spider):
    name = 'jobplanet'
    main_url = 'http://www.jobplanet.co.kr'
    
    custom_settings= {
        'DOWNLOADER_MIDDLEWARES': { 
            'crawler.middlewares.SeleniumMiddleware': 100 
        }
    }
    
    def get_company_names_from_DB(self) :
        #To Do : import sql
        test_company_names = ['무신사','고피자','알수없음']
        return test_company_names

    def start_requests(self):
        company_names = self.get_company_names_from_DB()
        for company_name in company_names:
            yield scrapy.Request(url=self.main_url+f'/search?query={company_name}',callback=self.parse_search_page,meta={'company_name':company_name})
            
    def parse_search_page(self,response) :
        company_key = response.css('#mainContents > div > div > div.result_company_card > div.is_company_card > div > a::attr(href)').get().split('/')[2] #첫번째가 가장 정확한 결과
        company_name_searched = response.css('#mainContents > div > div > div.result_company_card > div.is_company_card > div > a > b::text').get()
        print('company_key :',company_key,company_name_searched)
        detail_list = ['landing','reviews','salaries','interviews']
        for detail_item in detail_list :
            yield scrapy.Request(url=self.main_url+f'/companies/{company_key}/{detail_item}/{company_name_searched}',
                                 callback=self.parse_result_page,meta={'company_name' : response.meta['company_name'],
                                                                       'detail_item':detail_item})
    
    def parse_result_page(self,response) :
        doc = JobplanetItem()
        def get_landing_data(response) :
            landing_data = response.css('#contents_wrap > div > div > div > div > div.basic_info_sec > div > ul.basic_info_list > li > div > div > strong::text').getall()
            doc['company_sector'] = landing_data[0]
            doc['company_scale'] = landing_data[1]
            doc['company_employees'] = landing_data[2]
            doc['company_establishment_date'] = datetime.strptime(landing_data[3],'%Y.%m.%d').date()
            return doc
    
        def get_reviews_data(response) :
            review_count = int(response.css('#viewCompaniesMenu > ul > li.viewReviews > a > span::text').get())
            star_score = float(response.css('#premiumReviewStatistics > div > div > div > div.stats_smr_sec.left_sec > div.rate_star_wrap > span::text').get())
            doc['review_count'] = review_count
            doc['star_score'] = star_score
            return doc

        def get_salaries_data(response) :
            salary_count = int(response.css('#viewCompaniesMenu > ul > li.viewSalaries > a > span::text').get())
            salary_avg = int(response.css('#viewSalariesBanner > section.vsb_sec1 > div > span > strong::text').get().replace(',',''))
            doc['salary_count'] = salary_count
            doc['salary_avg'] = salary_avg
            return doc
            
        def get_interviews_data(response) :
            interview_count = int(response.css('#viewCompaniesMenu > ul > li.viewInterviews > a > span::text').get())
            interview_level = response.css('#viewInterviewsBanner > div > div > div.vib_gr_area > span::text').getall()
            interview_feel = response.css('#viewInterviewsBanner > div > div > div.vib_gf_w > div.tbl_ty2 > table > tbody > tr > td::text').getall()[:3]
            doc['interview_count'] = interview_count
            doc['interview_level'] = interview_level
            doc['interview_feel'] = interview_feel
            return doc
        get_data_func_dict = {'landing' : get_landing_data,
                              'reviews' : get_reviews_data,
                              'salaries' : get_salaries_data,
                              'interviews' : get_interviews_data}
        get_data_func_dict[response.meta['detail_item']](response)
        
        yield doc
        
        