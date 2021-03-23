# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    platform = scrapy.Field()
    
    logo_image = scrapy.Field()
    title = scrapy.Field()
    href = scrapy.Field()
    
    main_text = scrapy.Field()
    salary = scrapy.Field()
    skill_tag = scrapy.Field()
    sector = scrapy.Field()
    newbie = scrapy.Field()
    career = scrapy.Field()
    deadline = scrapy.Field()
    
    company_name = scrapy.Field()
    company_address = scrapy.Field()
    big_company = scrapy.Field()
    crawl_date = scrapy.Field()

class JobplanetItem(scrapy.Item) :
    # 잡플래닛 검색 데이터 저장
    company_name = scrapy.Field()
    company_sector = scrapy.Field()
    company_scale = scrapy.Field()
    company_employees = scrapy.Field()
    company_establishment_date = scrapy.Field()
    
    review_count = scrapy.Field()
    star_score = scrapy.Field()
    
    salary_count = scrapy.Field()
    salary_avg = scrapy.Field()
    
    interview_count = scrapy.Field()
    interview_level = scrapy.Field()
    interview_feel = scrapy.Field()
