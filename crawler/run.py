import scrapy
from multiprocessing import Process
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from crawler.spiders.programmers import ProgrammersSpider
from crawler.spiders.roketpunch import RoketpunchSpider
from crawler.spiders.wanted import WantedSpider
from crawler.spiders.kakao import KakaoSpider
from crawler.spiders.naver import NaverSpider

def start_spider(spider, settings: dict = {}, data: dict = {}):
    custom_settings= {
        'DOWNLOADER_MIDDLEWARES': { 
            'crawler.middlewares.SeleniumMiddleware': 100, 
        },

        # 'ITEM_PIPELINES' : {
        #     'crawler.pipelines.CrawlerPipeline': 300
        # }
    }
    def crawler_func():
        crawler_process = CrawlerProcess(custom_settings)
        crawler_process.crawl(spider)
        crawler_process.start()
    process = Process(target=crawler_func)
    process.start()
    return process

if __name__ == '__main__':
    map(lambda x: x.join(), [
        start_spider(WantedSpider),
        # start_spider(RoketpunchSpider),
        # start_spider(ProgrammersSpider),
        # start_spider(KakaoSpider),
        # start_spider(NaverSpider),
    ])
    exit()