from scrapy.crawler import CrawlerProcess
import os
import sys

path = __file__
path = os.path.split(path)[0]
path = os.path.split(path)[0]
sys.path.append(path)
from extract.site_crawler import HaloSpyder

if __name__ == "__main__":
    crawler = CrawlerProcess({"LOG_FILE": "/tmp/scrapy.log"})
    crawler.crawl(HaloSpyder)
    crawler.start()
