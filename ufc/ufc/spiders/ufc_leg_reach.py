import scrapy
import re
import pymysql
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from ufc.items import UfcItem
import boto3
import json

class ufc_leg_reach(scrapy.Spider):
    name = 'ufc_leg_reach'

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        AWS_ACCESS_KEY_ID = settings.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = settings.get("AWS_SECRET_ACCESS_KEY")
        return cls(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    def __init__(self, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
        self.AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
        self.AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY

        bucket = "kaija-betting"
        key = "db_credentials.json"

        s3 = boto3.resource(
            's3',
            aws_access_key_id = AWS_ACCESS_KEY_ID,
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY
        )

        obj = s3.Object(bucket, key)
        global credentials
        credentials = obj.get()['Body'].read().decode('utf-8')
        credentials = json.loads(credentials)

    def start_requests(self):

        conn = pymysql.connect(user=credentials['user'], passwd=credentials['password'], db=credentials['database'], host=credentials['host'], charset="utf8", use_unicode=True)
        cursor = conn.cursor()

        # Always bring back at least one ID for monitoring purposes.
        cursor.execute("""
            SELECT ufc_url, id FROM fighter_information
            WHERE ufc_url IS NOT NULL
            AND (leg_reach IS NULL OR leg_reach < 10)
            AND (marker NOT LIKE "%ufc.com%" OR marker IS NULL)
            OR id = "320"
        """)
        result = cursor.fetchall()

        for i in result:
            # Get ID of fighter to insert scraped value to correct fighter in pipelines.py
            item = UfcItem()
            item['id'] = int(i[1])
            request = scrapy.Request(str(i[0]), callback=self.parse)
            request.meta['item'] = item

            yield request

    def parse(self, response):

        item = UfcItem()
        item = response.meta['item']

        try:
            leg_reach_path = response.xpath("//div[@class='c-bio__label' and text() = 'Leg reach']")
            leg_reach = leg_reach_path.xpath("../div[@class='c-bio__text']/text()").extract_first()
            item['leg_reach'] = leg_reach
            return item

        except Exception as e:
            leg_reach = None
            print("Error:", e)
            pass
