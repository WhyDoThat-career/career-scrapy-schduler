# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.utils.python import to_bytes

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter

from selenium import webdriver
from selenium.webdriver import ActionChains
import time

class SeleniumMiddleware:
    
    @classmethod
    def from_crawler(cls, crawler):
        print("this is SeleniumMiddleware from_crawler +++++++++++++++++++++++")
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s
    
    def spider_opened(self, spider): #selenium 초기 설정
        chromedriver = './driver/chromedriver'
        headless_options = webdriver.ChromeOptions()
        headless_options.add_argument('headless')
        headless_options.add_argument('window')
        headless_options.add_argument('window-size=1920x1080')
        headless_options.add_argument("disable-gpu")
        headless_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.100 Safari/537.36")
        headless_options.add_argument("lang=ko_KR")

        driver = webdriver.Chrome(chromedriver,options=headless_options)
        self.driver = driver
        spider.logger.info('Spider opened: %s' % spider.name)
    
    def spider_closed(self,spider) :
        self.driver.close()
        
    def infinite_scroll_control(self) :
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        for i in range(10) :
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    
    def see_more_button_click(self):
        try :
            self.driver.find_element_by_class_name('see-more-text.primary.link').click()
            time.sleep(0.5)
            self.driver.find_element_by_class_name('see-more-text.primary.link').click()
            time.sleep(0.5)
        except :
            print('[Notice] 더보기 버튼이 없습니다.')
    
    def naver_more_button(self):
        while True :
            try :
                self.driver.find_element_by_class_name('more_btn').click()
                time.sleep(0.5)
            except :
                print('마지막 페이지까지 로드하였습니다.')
                break

    def process_request(self, request, spider):
        self.driver.get(request.url)
        if spider.name == 'wanted' and request.meta != {}:
            time.sleep(2)
            some_tag = self.driver.find_element_by_class_name('_3h_f6TfissC0l7ogPcn7lY')
            action = ActionChains(self.driver)
            action.move_to_element(some_tag).perform()
            time.sleep(1)
        else :
            time.sleep(1.2)
        if spider.name == 'wanted' and request.meta == {}:
            self.infinite_scroll_control()
        elif spider.name == 'roketpunch' and 'job_card_company' in request.meta :
            self.see_more_button_click()
        elif spider.name == 'naver' and request.meta == {} :
            self.naver_more_button()
            
        body = to_bytes(text=self.driver.page_source)
        return HtmlResponse(url = request.url, body = body, encoding='utf-8',request=request)

    def process_response(self, request, response, spider):

        return response

    def process_exception(self, request, exception, spider):

        pass

class CrawlerSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        print("this is CrawlerSpiderMiddleware from_crawler ===================")
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

class CrawlerDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)