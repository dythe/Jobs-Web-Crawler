import scrapy
from scrapy.item import Item, Field


class JobsItems(Item):

        title = Field()
        company_name = Field()
        #location = Field()
        jobdetailsurl = Field()

        position = Field()
        jobnature = Field()
        qualification = Field()
        job_description = Field()
        timestamp = Field()

        category = Field()
        subcategory = Field()
        source = Field()
        month = Field()