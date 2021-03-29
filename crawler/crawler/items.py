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
    name = scrapy.Field()
    sector = scrapy.Field()
    scale = scrapy.Field()
    employees = scrapy.Field()
    establishment_date = scrapy.Field()
    
    review_count = scrapy.Field()
    star_point = scrapy.Field()
    
    salary_count = scrapy.Field()
    salary_average = scrapy.Field()
    
    interview_count = scrapy.Field()
    interview_level = scrapy.Field()
    interview_feel = scrapy.Field()
