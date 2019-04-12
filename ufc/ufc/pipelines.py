import pymysql
import hashlib
import scrapy
from scrapy.exceptions import DropItem
from scrapy.http import Request
import boto3
import json

class MySQLStorePipeline(object):

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

        self.conn = pymysql.connect(user=credentials['user'], passwd=credentials['password'], db=credentials['database'], host=credentials['host'], charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):

        try:
            self.cursor.execute("""UPDATE fighter_information
             SET leg_reach = %s,
             marker = CONCAT(COALESCE(marker,''), ' ufc.com')
             WHERE id = %s
             AND (leg_reach IS NULL OR leg_reach < 10)
              """,
            (item['leg_reach'],
             item['id']))

            self.conn.commit()

            print(self.cursor.fetchone())

        except pymysql.Error as e:
            print("Error: {}".format(e.args[0]))
            print(e.args[1])

        return item
