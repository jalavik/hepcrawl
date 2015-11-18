# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2015 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Spider for BASE."""

from __future__ import absolute_import, print_function

import os
import urllib
import re

from scrapy import Request, Selector
from scrapy.spiders import XMLFeedSpider, CrawlSpider
from scrapy.utils.iterators import _body_or_str
from scrapy.utils.python import re_rsearch

from ..items import HEPRecord
from ..loaders import HEPLoader



#class AlphaSpider(XMLFeedSpider):
class AlphaSpider(CrawlSpider):
    
    """BASE crawler
    Scrapes BASE metadata XML files one at a time. 
    The actual files should be retrieved from BASE viat its OAI interface. 

    This spider takes one BASE metadata record which are stored in an XML file.

    1. First it looks through the file and determines if it has a direct link to a fulltext pdf. 
       (Actually it doesn't recognize fulltexts; it's happy when it sees a pdf of some kind.)
       calls: parse_node()

    2. If no direct link exists, it scrapes the urls it found and tries to find a direct pdf 
       link using scrape_for_pdf(). Whatever happens, it will then call parse_node() again 
       and goes through the XML file and extracts all the desired data.
       calls: scrape_for_pdf()

    3. parse_node() will be called a second time and after parsing it will return a HEPrecord Item().
       That item will be put trough a JSON writer pipeline.
       calls: parse_node() again, then sends to processing pipeline.
       
    Duplicate requests filters have been manually disabled for us to be able to call 
    parse_node() twice: 
    'DUPEFILTER_CLASS' : 'scrapy.dupefilters.BaseDupeFilter'
    Better way to do this??


    Example usage:
    scrapy crawl alpha -s "JSON_OUTPUT_DIR=tmp/"
    scrapy crawl alpha -a source_file=file://`pwd`/tests/responses/alpha/test_alpha.htm -s "JSON_OUTPUT_DIR=tmp/"

    
    TODO:
    *Namespaces not working, namespace removal not working. Test case has been stripped of namespaces manually :|
    *Should the spider also access the BASE OAI interface and retrieve the XMLs?
     Now it's assumed that the XMLs are already in place in some local directory.
    *When should the pdf page numbers be counted? Maybe it's not sensible to do it here. 
    *Why is the JSON pipeline not writing unicode?
    *Some Items missing
    *Needs more testing with different XML files!


    Happy crawling!
    """

    name = 'alpha'
    start_urls = ["http://alpha.web.cern.ch/publications#thesis"]
    #pdf_link = [] #this will contain the direct pdf link, should be accessible from everywhere
    #namespaces = [('dc', "http://purl.org/dc/elements/1.1/"), ("base_dc", "http://oai.base-search.net/base_dc/")] #are these necessary?
    #iterator = 'iternodes'  # This is actually unnecessary, since it's the default value, REMOVE?
    itertag = "//div[@class = 'node node-thesis']"
    author_data = []
    
    
    ##titles:
    #response.xpath("//div[@class = 'node node-thesis']/div[@class = 'node-headline clearfix']//a/text()").extract()

    ##pdfs:
    #response.xpath("//div[@class = 'node node-thesis']/div[@class = 'content clearfix']//span[@class='file']/a/@href").extract()
    
    ##authors:
    #response.xpath("//div[@class = 'node node-thesis']/div[@class = 'content clearfix']//div[@class='field-item even']//p[last()]/text()").extract()
    ##abstracts:
    #response.xpath("//div[@class = 'node node-thesis']/div[@class = 'content clearfix']//div[@class='field-item even']//p[position()<last()]/text()").extract()
    
    
    
    
    #custom_settings = {
        ##'ITEM_PIPELINES': {'HEPcrawl_BASE.pipelines.HepCrawlPipeline': 100,},
        ##'ITEM_PIPELINES': {'HEPcrawl_BASE.pipelines.JsonWriterPipeline': 100,} #use this, could be modified a bit though
        ##'DUPEFILTER_DEBUG' : True,
        #'DUPEFILTER_CLASS' : 'scrapy.dupefilters.BaseDupeFilter' #THIS WAY YOU CAN SCRAPE TWICE!! otherwise duplicate requests are filtered
        #}
    ##scrapy.dupefilter.BaseDupeFilteris deprecated, use `scrapy.dupefilters` instead

    
    
    
    def __init__(self, source_file=None, *args, **kwargs):
        """Construct BASE spider"""
        super(AlphaSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file
        self.target_folder = "tmp/"
        if not os.path.exists(self.target_folder):
            os.makedirs(self.target_folder)

    #def start_requests(self):
        #"""Default starting point for scraping shall be the web page"""
        #yield Request(self.source_file)
    
    
    def split_fullname(self, author):
        """If we want to split the author name to surname and given names.
        Is this necessary? Could we use only the full_name key in the authors-dictionary?
        """
        import re
        fullname = author.split()
        surname = fullname[-1] #assuming surname comes last...
        given_names = " ".join(fullname[:-1])
        return surname, given_names
    
    #detects if a string contains numbers
    def has_numbers(self, s):
        return any(char.isdigit() for char in s)
    
    def parse_author_data(self, author_line):
        """Parses the line where there are data about the author(s) 
        """
        self.author_data = []
        #huom muuta tämä niin että on vain yksi authori tässä
        print("PARSITAAN AUTHOR DATAA")

        author_list = re.sub(r'[\n\t\xa0]', '', author_line).split(",")  #remove unwanted characters
        author = author_list[0]
        surname, given_names = self.split_fullname(author)
        
        for i in author_list:
            if "thesis" in i.lower():
                thesis_type = i.strip()#there might be some unwanted whitespaces
            if "university" in i.lower():
                affiliation = re.sub(r"[^A-Za-z\s]+", '', i).strip() #affiliation element might include the year
            if self.has_numbers(i):
                year = re.findall(r'\d+', i)[0].strip()

        self.author_data.append({
                                'fullname': author,
                                'surname': surname,
                                'given_names': given_names,
                                'thesis_type': thesis_type,
                                'affiliation': affiliation,
                                'year': year
                                }) 

       
        
    def get_authors(self):
        """Gets the desired elements from author_data, 
        these will be put in the scrapy item
        """
        authors = []
        for author in self.author_data:
            authors.append({
                        'surname': author['surname'],
                        'given_names': author['given_names'], #this must be a string?
                        #'full_name': author.extract(), #should we only use full_name? 
                        'affiliations': [author['affiliation']] #this must be a list?
                        })
        
        return authors
   
    #abstracts are divided in multiple abs_paragraphs
    #this way we can just merge the paragraphs and input this to HEPloader
    #if we don't do this, HEPLoader takes just the first paragraph
    #other way would be to make an AlphaLoader and override
    #HEPloaders abstract_out = TakeFirst()
    #This is simple though
    def get_abstract(self, abs_pars):
        whole_abstract = [" ".join(abs_pars)]
        return whole_abstract
        
       


    def parse(self, response):
        """Parse Alpha web page into a HEP record."""
        
        #create scrapy Items and send them to the pipeline
        response =  response.replace(body=response.body.replace('<br />', '')) #random <br>'s will create problems!
        node = Selector(response)   
        
        for thesis in node.xpath(self.itertag):
            record = HEPLoader(item=HEPRecord(), selector=thesis, response=response)  
            author_line = thesis.xpath(
                "./div[@class = 'content clearfix']//div[@class='field-item even']"
                "/p[contains(text(),'Thesis')]/text()"
                ).extract()
            #author_line looks like this: 
            #u'Chukman So, PhD Thesis, University of California, Berkeley (2014)'
            self.parse_author_data(author_line[0])
            authors = self.get_authors()
            record.add_value('authors', authors)
            
            record.add_xpath('title', "./div[@class = 'node-headline clearfix']//a/text()")
            abs_paragraphs = thesis.xpath(
                "./div[@class = 'content clearfix']//div[@class='field-item even']"
                "/p[normalize-space()][string-length(text()) > 0][position() < last()]/text()"
                ).extract()
            abstract = self.get_abstract(abs_paragraphs)
            
            record.add_value("abstract", abstract)

            
            record.add_value('date_published', self.author_data[0]['year'])
            record.add_xpath('files', "./div[@class = 'content clearfix']//span[@class='file']/a/@href")
            record.add_value('source', 'Alpha experiment')
            yield record.load_item()


    


