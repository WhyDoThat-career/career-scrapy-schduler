import functools
import time
from datetime import datetime
import schedule
import scrapy
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from crawler.spiders.programmers import ProgrammersSpider
from crawler.spiders.roketpunch import RoketpunchSpider
from crawler.spiders.wanted import WantedSpider
from crawler.spiders.kakao import KakaoSpider
from crawler.spiders.naver import NaverSpider
from crawler import sql_db

def print_elapsed_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_timestamp = time.time()
        print(f'LOG: Running job "{func.__name__}" at {datetime.now()}')
        result = func(*args, **kwargs)
        print(f'LOG: Job "{func.__name__}" completed in {time.time() - start_timestamp} seconds')
        print(f'LOG: Finished job "{func.__name__}" at {datetime.now()}')
        return result

    return wrapper

def start_spider():
    custom_settings= {
        'DOWNLOADER_MIDDLEWARES': { 
            'crawler.middlewares.SeleniumMiddleware': 100, 
        },

        'ITEM_PIPELINES' : {
            'crawler.pipelines.CrawlerPipeline': 300
        }
    }
    def crawler_func():
        configure_logging()
        runner = CrawlerRunner(custom_settings)

        @defer.inlineCallbacks
        def crawl() :
            yield runner.crawl(KakaoSpider)
            yield runner.crawl(NaverSpider)
            yield runner.crawl(ProgrammersSpider)
            yield runner.crawl(RoketpunchSpider)
            yield runner.crawl(WantedSpider)
            reactor.stop()
        crawl()
        reactor.run() 
    crawler_func()

def start_jobplanet():
    custom_settings= {
        'DOWNLOADER_MIDDLEWARES': { 
            'crawler.middlewares.SeleniumMiddleware': 100, 
        },

        'ITEM_PIPELINES' : {
            'crawler.pipelines.JobPlanetPipeline': 300
        }
    }
    def crawler_func():
        configure_logging()
        runner = CrawlerRunner(custom_settings)

        @defer.inlineCallbacks
        def crawl() :
            yield runner.crawl(JobplanetSpider)
            reactor.stop()
        crawl()
        reactor.run() 
    crawler_func()

@print_elapsed_time
def job_every_day_crawl() :
    start_spider()

@print_elapsed_time
def job_remove_at_database() :
    pass

@print_elapsed_time
def job_every_month_crawl() :
    start_jobplanet()

schedule.every().day.at("15:02").do(job_every_day_crawl)
schedule.every().day.at("23:00").do(job_remove_at_database)
# schedule.every().week.at("2").do(job_every_month_crawl)

# job_every_day_crawl()
while True:
    schedule.run_pending()
    time.sleep(1)