import scrapy

class UfcItem(scrapy.Item):
    id = scrapy.Field()
    leg_reach = scrapy.Field()