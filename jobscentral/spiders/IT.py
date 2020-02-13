import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.item import Item, Field
from jobscentral.items import JobsItems
import string
from datetime import datetime, date, time, timedelta
from urlparse import urljoin
from scrapy.utils.response import get_base_url
import time
import re
from scrapy.http import Request

class IT(CrawlSpider):
    name = 'IT'

    allowed_domains = ["jobscentral.com.sg"]
    custom_settings = {
                       'TOR_RENEW_IDENTITY_ENABLED': True,
                       'TOR_ITEMS_TO_SCRAPE_PER_IDENTITY': 20
                       }

    download_delay = 6
    handle_httpstatus_list = [301, 302]

    rules = (
        #Rule(SgmlLinkExtractor(allow=(), restrict_xpaths=('//li[@class="page-item"]/a[@aria-label="Next"]',)), callback="self.parse", follow=True),
        Rule(SgmlLinkExtractor(allow=(), restrict_xpaths=('//li[@class="page-item"]/a[@aria-label="Next"]',)), callback="parse_item", follow=True),
        
    )

    start_urls = [
            #### Legend:
            #### ## = Done
            #### # = Not Done

            # A
            'https://jobscentral.com.sg/jobs-accounting?orderby=-Date' # 20+ pages
            'https://jobscentral.com.sg/jobs-advertising?orderby=-Date', # 40 + pages
            'https://jobscentral.com.sg/jobs-architecture?orderby=-Date', # 40+ pages
            'https://jobscentral.com.sg/jobs-administrative?orderby=-Date', # 90+ pages
            'https://jobscentral.com.sg/jobs-agriculture?orderby=-Date', #
            'https://jobscentral.com.sg/jobs-arts?orderby=-Date' # <10 pages

            # B-C
            'https://jobscentral.com.sg/jobs-banking?orderby=-Date',
            'https://jobscentral.com.sg/jobs-computer?orderby=-Date',
            'https://jobscentral.com.sg/jobs-construction?orderby=-Date',
            'https://jobscentral.com.sg/jobs-consulting?orderby=-Date',
            'https://jobscentral.com.sg/jobs-customer-service?orderby=-Date',

            # E-F
            'https://jobscentral.com.sg/jobs-education?orderby=-Date',
            'https://jobscentral.com.sg/jobs-energy?orderby=-Date',
            'https://jobscentral.com.sg/jobs-engineering?orderby=-Date',
            'https://jobscentral.com.sg/jobs-facilities?orderby=-Date',
            'https://jobscentral.com.sg/jobs-finance?orderby=-Date',
            'https://jobscentral.com.sg/jobs-food-service?orderby=-Date'
 
            # H-I-L
            'https://jobscentral.com.sg/jobs-hospitality?orderby=-Date',
            'https://jobscentral.com.sg/jobs-healthcare?orderby=-Date',
            'https://jobscentral.com.sg/jobs-human-resources?orderby=-Date',
            'https://jobscentral.com.sg/jobs-it?orderby=-Date',
            'https://jobscentral.com.sg/jobs-information-technology?orderby=-Date',
            'https://jobscentral.com.sg/jobs-insurance?orderby=-Date',
            'https://jobscentral.com.sg/jobs-legal?orderby=-Date',
            'https://jobscentral.com.sg/jobs-logistics?orderby=-Date',
            'https://jobscentral.com.sg/jobs-loans?orderby=-Date',

            # M-P
            'https://jobscentral.com.sg/jobs-management?orderby=-Date',
            'https://jobscentral.com.sg/jobs-manufacturing?orderby=-Date',
            'https://jobscentral.com.sg/jobs-maritime?orderby=-Date',
            'https://jobscentral.com.sg/jobs-marketing?orderby=-Date',
            'https://jobscentral.com.sg/jobs-mechanical?orderby=-Date',
            'https://jobscentral.com.sg/jobs-pr?orderby=-Date',
            'https://jobscentral.com.sg/jobs-public-relations?orderby=-Date',
            'https://jobscentral.com.sg/jobs-pharmaceutical?orderby=-Date', #3~ pages
            'https://jobscentral.com.sg/jobs-publishing?orderby=-Date'

            # R-S
            'https://jobscentral.com.sg/jobs-real-estate?orderby=-Date',
            'https://jobscentral.com.sg/jobs-restaurant?orderby=-Date',
            'https://jobscentral.com.sg/jobs-retail?orderby=-Date',
            'https://jobscentral.com.sg/jobs-sales?orderby=-Date',
            'https://jobscentral.com.sg/jobs-scientific?orderby=-Date',
            'https://jobscentral.com.sg/jobs-security?orderby=-Date',
            'https://jobscentral.com.sg/jobs-social-care?orderby=-Date'

            # T-V
            'https://jobscentral.com.sg/jobs-telecommunications?orderby=-Date',
            'https://jobscentral.com.sg/jobs-training?orderby=-Date',
            'https://jobscentral.com.sg/jobs-transporation?orderby=-Date',
            'https://jobscentral.com.sg/jobs-travel?orderby=-Date',
            'https://jobscentral.com.sg/jobs-volunteering?orderby=-Date'
    ]

    first_response = True

    def parse(self, response):
        if self.first_response == True:
        # use it or pass it to some other function
            for r in self.parse_item(response):
                yield r
            self.first_response == False

        # Pass the response to crawlspider 
        for r in super(IT, self).parse(response):
            yield r

    def parse_item(self, response):
        
        self.logger.info("Response %d for %r" % (response.status, response.url))
        #self.logger.info("base url %s", get_base_url(response))
      
        items = []

        self.logger.info("Visited Outer Link %s", response.url)

        
        for loop in response.xpath('//div[@class="col-md-11"]'):

            item = JobsItems()

            #t = loop.xpath('./div[@class="col-xs-12 col-md-3 px-0"]/div[@class="posted-date text-muted hidden-sm-down"]//text()').extract()[1].strip()
            t = loop.xpath('./div[@class="col-xs-12 col-md-3 px-0"]/div[@class="posted-date text-muted hidden-sm-down"]//text()').extract()[1].strip()
            
            #self.logger.info("T VALUE: %s", t)
            t = datetime.strptime(t, '%d-%b-%y')

            item['timestamp'] = t

            titRem = loop.xpath('./div[@class="col-xs-9"]/h4/a/text()').extract_first()
            #self.logger.info("titRem value: %s", titRem)
            titRem = titRem.replace('\r', '')
            titRem = titRem.replace('\t', '')
            titRem = titRem.replace('\n', '')
            titRem = titRem.replace('"', '')
            titRem = titRem.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0','')
            item['title'] = titRem.strip()

            item['jobdetailsurl'] = 'https://jobscentral.com.sg' + loop.xpath('./div[@class="col-xs-9"]/h4/a/@href').extract()[0]

            companyPt1 = loop.xpath('./div[@class="col-xs-9"]/h6/span/text()').extract_first()
            companyPt2 = loop.xpath('./div[@class="col-xs-9"]/h6/a/text()').extract_first()
            if not companyPt1:
                #companyPt2 = companyPt2.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0','')
                item['company_name'] = companyPt2
            else:
                #companyPt1 = companyPt1.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0','')
                item['company_name'] = companyPt1
            
            if 'jobscentral' in str(get_base_url(response)):
                item['source'] = 'jobscentral'

            if 'jobs-accounting' in str(response.request.url):
                item['category'] = 'Accounting'
            elif 'jobs-advertising' in str(response.request.url):
                item['category'] = 'Advertising'
            elif 'jobs-architecture' in str(response.request.url):
                item['category'] = 'Architecture'
            elif 'jobs-administrative' in str(response.request.url):
                item['category'] = 'Administrative'
            elif 'jobs-agriculture' in str(response.request.url):
                item['category'] = 'Agriculture'
            elif 'jobs-arts' in str(response.request.url):
                item['category'] = 'Arts' #END OF A
            elif 'jobs-banking' in str(response.request.url):
                item['category'] = 'Banking'
            elif 'jobs-computer' in str(response.request.url):
                item['category'] = 'Computer'
            elif 'jobs-construction' in str(response.request.url):
                item['category'] = 'Construction'
            elif 'jobs-consulting' in str(response.request.url):
                item['category'] = 'Consulting'
            elif 'jobs-customer-service' in str(response.request.url):
                item['category'] = 'Customer Service'
            elif 'jobs-education' in str(response.request.url):
                item['category'] = 'Education'
            elif 'jobs-energy' in str(response.request.url):
                item['category'] = 'Energy'
            elif 'jobs-engineering' in str(response.request.url):
                item['category'] = 'Engineering'
            elif 'jobs-facilities' in str(response.request.url):
                item['category'] = 'Facilities'
            elif 'jobs-finance' in str(response.request.url):
                item['category'] = 'Finance'
            elif 'jobs-food-service' in str(response.request.url):
                item['category'] = 'Food Service'
            elif 'jobs-hospitality' in str(response.request.url):
                item['category'] = 'Hospitality'
            elif 'jobs-healthcare' in str(response.request.url):
                item['category'] = 'Healthcare'
            elif 'jobs-human-resources' in str(response.request.url):
                item['category'] = 'Human Resources'
            elif 'jobs-it' in str(response.request.url):
                item['category'] = 'Information Technology'
            elif 'jobs-information-technology' in str(response.request.url):
                item['category'] = 'Information Technology'            
            elif 'jobs-insurance' in str(response.request.url):
                item['category'] = 'Insurance'
            elif 'jobs-legal' in str(response.request.url):
                item['category'] = 'Legal'
            elif 'jobs-logistics' in str(response.request.url):
                item['category'] = 'Logistics'                
            elif 'jobs-loans' in str(response.request.url):
                item['category'] = 'Loans'
            elif 'jobs-management' in str(response.request.url):
                item['category'] = 'Management'
            elif 'jobs-manufacturing' in str(response.request.url):
                item['category'] = 'Manufacturing'
            elif 'jobs-maritime' in str(response.request.url):
                item['category'] = 'Maritime'                
            elif 'jobs-marketing' in str(response.request.url):
                item['category'] = 'Marketing'
            elif 'jobs-mechanical' in str(response.request.url):
                item['category'] = 'Mechanical'
            elif 'jobs-pr' in str(response.request.url):
                item['category'] = 'Public Relations'                
            elif 'jobs-public-relations' in str(response.request.url):
                item['category'] = 'Public Relations'
            elif 'jobs-pharmaceutical' in str(response.request.url):
                item['category'] = 'Pharmaceutical'                
            elif 'jobs-publishing' in str(response.request.url):
                item['category'] = 'Publishing'
            elif 'jobs-real-estate' in str(response.request.url):
                item['category'] = 'Real Estate'
            elif 'jobs-restaurant' in str(response.request.url):
                item['category'] = 'Restaurant'
            elif 'jobs-retail' in str(response.request.url):
                item['category'] = 'Retail'                
            elif 'jobs-sales' in str(response.request.url):
                item['category'] = 'Sales'
            elif 'jobs-scientific' in str(response.request.url):
                item['category'] = 'Scientific'
            elif 'jobs-security' in str(response.request.url):
                item['category'] = 'Security'                
            elif 'jobs-social-care' in str(response.request.url):
                item['category'] = 'Social Care'
            elif 'jobs-telecommunications' in str(response.request.url):
                item['category'] = 'Telecommunications'
            elif 'jobs-training' in str(response.request.url):
                item['category'] = 'Training'
            elif 'jobs-transport' in str(response.request.url):
                item['category'] = 'Transporat'                
            elif 'jobs-travel' in str(response.request.url):
                item['category'] = 'Travel'
            elif 'jobs-volunteering' in str(response.request.url):
                item['category'] = 'Volunteering'

            request = scrapy.Request(item['jobdetailsurl'], callback=self.parse_jobdetails, dont_filter=True)
            request.meta['item'] = item

            yield request

        # next_page = response.xpath('//li[@class="page-item"]/a[@aria-label="Next"]/@href').extract_first()
        # self.logger.info("Next Page: %s", next_page)

        # if next_page:
        #     base_url = get_base_url(response)
        #     next_page = urljoin(base_url,next_page)
        #     yield scrapy.Request(next_page, callback=self.parse, dont_filter=True)

    def parse_jobdetails(self, response):
        
        self.logger.info('Visited Internal Link %s', response.url)
        print response
        item = response.meta['item']
        item = self.getJobInformation(item, response)
        return item


    def getJobInformation(self, item, response):
        trans_table = {ord(c): None for c in u'\r\n\t\u00a0'}

        #detect whether page countains a unique standard element of the standard jobscentral page
        uniquefield = ''.join(response.xpath('/html/body/app-root/div/job-detail/div/div[3]/div/div[2]/div[1]/div/div/div/div[1]/save-job/div/div/span/text()').extract()).strip()
        uniquefield2 = ''.join(response.xpath('/html/body/app-root/div/job-detail/div/div[3]/div/div[1]/div[1]/div/div/div/div[1]/save-job/div/div/span/text()').extract()).strip()
        
        # self.logger.info("UniqueField: %s", uniquefield)
        # self.logger.info("UniqueField2: %s", uniquefield2)

        if 'Save Job' in uniquefield or uniquefield2:
            self.logger.info("Normal Page")

            item['jobnature'] = ''
            item['position'] = ''
            item['qualification'] = ''
            item['subcategory'] = ''

            for three in response.xpath('//job-snapshot/dl/div'):
                if three.xpath('./dt[contains(text(), "Job Nature:")]'):
                    item['jobnature'] = three.xpath('./dd/text()').extract_first()

                if three.xpath('./dt[contains(text(), "Position Level:")]'):
                    item['position'] = three.xpath('./dd/text()').extract_first()

                # if three.xpath('./dt[contains(text(), "Job Category:")]'):
                #     item['subcategory'] = three.xpath('./dd/text()').extract_first()

                if three.xpath('./dt[contains(text(), "Job Category:")]'):
                    subcat = ' '.join(three.xpath('./dd/text()').extract()).strip()
                    subcat = subcat.replace('\r', '')
                    subcat = subcat.replace('\t', '')
                    subcat = subcat.replace('\n', '')
                    subcat = subcat.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0', '').replace(u'\u2014', '')
                    subcat = re.sub("\s\s+" , " ", subcat)

                    subcat2 = ' '.join(three.xpath('./dd/span/a/text()').extract()).strip()
                    subcat2 = subcat2.replace('\r', '')
                    subcat2 = subcat2.replace('\t', '')
                    subcat2 = subcat2.replace('\n', '')
                    subcat2 = subcat2.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0', '').replace(u'\u2014', '')
                    subcat2 = re.sub("\s\s+" , " ", subcat2)

                    item['subcategory'] = subcat + subcat2

                if three.xpath('./dt[contains(text(), "Qualification:")]'):
                    item['qualification'] = three.xpath('./dd/text()').extract_first()

            for jobdescription in response.xpath('//div[@class="job-description-container"]'):
                j = ' '.join(jobdescription.xpath(".//div[@class='hidden-md-up']//text()").extract()).strip()
                j = j.replace('\r', '')
                j = j.replace('\t', '')
                j = j.replace('\n', '')
                j = j.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0', '').replace(u'\u2014', '')
                #j = jobdescription.xpath(".//div[@class='hidden-md-up']//text()").extract()
                j = j.strip()
                j = re.sub("\s\s+" , " ", j)

                d = ' '.join(jobdescription.xpath(".//div[@class='hidden-sm-down']//text()").extract()).strip()
                d = d.replace('\r', '')
                d = d.replace('\t', '')
                d = d.replace('\n', '')
                d = d.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0', '').replace(u'\u2014', '')
                d = d.strip()
                d = re.sub("\s\s+" , " ", d)

                item['job_description'] = j + d

        else:
            self.logger.info("Unusual Page")
   
            loopjob = response.xpath('//div[@class="job-description-container"]')
            
            if not loopjob:
                item['job_description'] = ''
            else:
                for jobdescription in loopjob:
                    j = ' '.join(jobdescription.xpath(".//div[@class='hidden-md-up']//text()").extract()).strip()
                    j = j.replace('\r', '')
                    j = j.replace('\t', '')
                    j = j.replace('\n', '')
                    j = j.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0', '').replace(u'\u2014', '')
                    j = j.strip()
                    j = re.sub("\s\s+" , " ", j)
                    #j = jobdescription.xpath(".//div[@class='hidden-md-up']//text()").extract()
                    
                    d = ' '.join(jobdescription.xpath(".//div[@class='hidden-sm-down']//text()").extract()).strip()
                    d = d.replace('\r', '')
                    d = d.replace('\t', '')
                    d = d.replace('\n', '')
                    d = d.replace(u"\u201c", "").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"\u2022", "").replace(u"\u201d", "").replace(u"\uf0b7", "").replace(u'\u2013','').replace(u'\xa1','').replace(u'\xa6s','').replace(u'\xa0', '').replace(u'\u2014', '')
                    d = d.strip()
                    d = re.sub("\s\s+" , " ", d)
                    item['job_description'] = j + d
            
            item['subcategory'] = ''
            item['jobnature'] = ''
            item['position'] = ''
            item['qualification'] = ''

        return item

    def ifNotEmptyGetIndex(self, item, index = 0):
        if item: #check to see it's not empty
            return item[index]
        else:
            return item