from time import sleep
import pandas as pd
import scrapy
from scrapy.linkextractors import LinkExtractor
from sqlalchemy import create_engine, text
import extract.page_crawler as page_crawler
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from shutil import which
from datetime import datetime
from db_tools.db_tools import DBTools


class HaloSpyder(scrapy.Spider):
    name = "halooglasi.com"
    allowed_domains = ["halooglasi.com"]
    start_urls = ["https://www.halooglasi.com/nekretnine/izdavanje-stanova"]

    def __init__(self, name=None, search_date=None, **kwargs):
        super().__init__(name, **kwargs)
        if search_date == None:
            self.search_date = HaloSpyder.__get_latest_date()
        else:
            self.search_date = search_date

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 2,
        "FEEDS": {
            "/tmp/etl/raw_output.csv": {
                "format": "csv",
                "encoding": "utf8",
                "overwrite": True,
            }
        },
        "SELENIUM_DRIVER_NAME": "firefox",
        "SELENIUM_DRIVER_EXECUTABLE_PATH": "geckodriver",
        "SELENIUM_DRIVER_ARGUMENTS": ["-headless"],
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_selenium.SeleniumMiddleware": 800,
        },
    }
    active_page = 1

    stop_scraping = False

    def parse(self, response):
        listing_links = HaloSpyder.__get_listing_pages(response)
        if not listing_links or self.stop_scraping or self.active_page > 15:
            print(f"Crawler finished on page: {self.active_page}")
            return
        for listing in listing_links:
            listing_url = listing.url
            print("Up to selenium request")
            yield SeleniumRequest(
                url=listing_url, callback=self.__parse_page, wait_time=1
            )
        self.active_page += 1
        next_page_url = self.start_urls[0]
        next_page_url = next_page_url + f"?page={self.active_page}"
        print(f"Moving to page: {self.active_page}")
        yield response.follow(url=next_page_url, callback=self.parse)

    @staticmethod
    def __get_listing_pages(response):
        allow_regex = "izdavanje-stanova\/[\w-]*\/\d*"
        restrict_css = "h3.product-title > a"
        link_extractor = LinkExtractor(
            allow=allow_regex,
            unique=True,
            allow_domains=HaloSpyder.allowed_domains,
            restrict_css=restrict_css,
        )
        links = link_extractor.extract_links(response)
        return links

    def __parse_page(self, response):
        parsed_page = None
        try:
            parsed_page = page_crawler.parse_page(response)
        except ConnectionRefusedError as e:
            print(e)
            sleep(20)
            parsed_page = page_crawler.parse_page(response)
        date_posted = HaloSpyder.__get_date_posted(parsed_page["date_posted"])
        if date_posted is not None and date_posted < self.search_date:
            self.stop_scraping = True
        return parsed_page

    @staticmethod
    def __get_date_posted(raw_date):
        try:
            unformatted_date = raw_date[:10]
            formatted_date = datetime.strptime(unformatted_date, "%d.%m.%Y").date()
            return formatted_date
        except Exception:
            return None

    @staticmethod
    def __get_latest_date():
        conn_string = DBTools.get_connection_string()
        engine = create_engine(conn_string)
        query = "SELECT MAX(date_posted) as latest_date from real_estate"
        query = text(query)
        try:
            with engine.connect() as con:
                response = con.execute(query)
                for row in response:
                    latest_date = row[0]
            latest_date = datetime.strptime(latest_date, "%d.%m.%Y")
            latest_date = latest_date.date()
        except:
            latest_date = datetime.today().date()
        return latest_date
