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
#import urllib
import re
import sys

from scrapy import Request, Selector
from scrapy.spiders import CrawlSpider
#from scrapy.utils.iterators import _body_or_str
#from scrapy.utils.python import re_rsearch

from ..items import HEPRecord
from ..loaders import HEPLoader

class AlphaSpider(CrawlSpider):
    
    """Alpha crawler
    Scrapes theses metadata from Alpha experiment web page.
    http://alpha.web.cern.ch/publications#thesis
    
    Desired information are in the following elements:
    1. Titles:
    "//div[@class = 'node node-thesis']/div[@class = 'node-headline clearfix']//a/text()"
    
    2. Authors:
    "//div[@class = 'node node-thesis']/div[@class = 'content clearfix']"
    "//div[@class='field-item even']/p[contains(text(),'Thesis')]/text()"
    
    3. Abstracts:
    "//div[@class = 'node node-thesis']/div[@class = 'content clearfix']"
    "//div[@class='field-item even']/p[normalize-space()][string-length(text()) > 0][position() < last()]/text()"
    
    4. PDF links:
    "//div[@class = 'node node-thesis']/div[@class = 'content clearfix']//span[@class='file']/a/@href"


    Example usage:
    scrapy crawl alpha -s "JSON_OUTPUT_DIR=tmp/"
    scrapy crawl alpha -a source_file=file://`pwd`/tests/responses/alpha/test_alpha.htm -s "JSON_OUTPUT_DIR=tmp/"

    
    TODO:
    *When should the pdf page numbers be counted? Maybe it's not sensible to do it here. 
    *Why is the JSON pipeline not writing unicode?
    *Some Items missing
    *Needs more testing with different XML files!


    Happy crawling!
    """

    name = 'alpha'
    start_urls = ["http://alpha.web.cern.ch/publications#thesis"]
    itertag = "//div[@class = 'node node-thesis']"
    author_data = []
     
    
    
    def __init__(self, source_file=None, *args, **kwargs):
        """Construct Alpha spider"""
        super(AlphaSpider, self).__init__(*args, **kwargs)
        self.source_file = source_file
        self.target_folder = "tmp/"
        if not os.path.exists(self.target_folder):
            os.makedirs(self.target_folder)

    def start_requests(self):
        """You can test the spider with local files with this.
        Comment this function when crawling the web page."""
        yield Request(self.source_file)
    
    
    def split_fullname(self, author):
        """If we want to split the author name to surname and given names.
        Is this necessary? Could we use only the full_name key in the authors-dictionary?
        """
        import re
        fullname = author.split()
        surname = fullname[-1] #assuming surname comes last...
        given_names = " ".join(fullname[:-1])
        return surname, given_names
    
    def has_numbers(self, s):
        """Detects if a string contains numbers"""
        return any(char.isdigit() for char in s)
    
    def parse_author_data(self, author_line):
        """Parses the line where there are data about the author(s) 
        """
        
        #we must do this everytime this function is called; 
        #otherwise records are just appended on top
        #of each other:
        author_data = [] 
        print("PARSITAAN AUTHOR DATAA")

        author_list = re.sub(r'[\n\t\xa0]', '', author_line).split(",")  #remove unwanted characters
        
        #try:
            #assert author_line
        #except AssertionError:
            
            
        author = author_list[0]
        surname, given_names = self.split_fullname(author)
        
        for i in author_list:
            if "thesis" in i.lower():
                thesis_type = i.strip()#there might be some unwanted whitespaces
            if "university" in i.lower():
                affiliation = re.sub(r"[^A-Za-z\s]+", '', i).strip() #affiliation element might include the year
            if self.has_numbers(i):
                year = re.findall(r'\d+', i)[0].strip()

        author_data.append({
                                'fullname': author,
                                'surname': surname,
                                'given_names': given_names,
                                'thesis_type': thesis_type,
                                'affiliation': affiliation,
                                'year': year
                                }) 
        return author_data

       
        
    def get_authors(self):
        """Gets the desired elements from author_data, 
        these will be put in the scrapy author item
        """
        try:
            assert self.author_data
        except AssertionError:
            print("AssertionError: "
                "You must call self.parse_author_data(author_line[0]) "
                "before calling get_authors()!")
        
        authors = []
        for author in self.author_data:
            authors.append({
                        'surname': author['surname'],
                        'given_names': author['given_names'], #this must be a string?
                        #'full_name': author.extract(), #should we only use full_name? 
                        'affiliations': [author['affiliation']] #this must be a list?
                        })
        
        return authors
   
    
    def get_abstract(self, abs_pars):
        """Abstracts are divided in multiple paragraphs.
        This way we can just merge the paragraphs and input this to HEPloader
        if we don't do this, HEPLoader takes just the first paragraph.
        Other way would be to make an AlphaLoader and override
        HEPloaders abstract_out = TakeFirst().
        This is better though.
        """
        whole_abstract = [" ".join(abs_pars)]
        return whole_abstract
        
       


    def parse(self, response):
        """Parse Alpha web page into a HEP record."""
        
        #random <br>'s will create problems:
        response =  response.replace(body=response.body.replace('<br />', '')) 
        node = Selector(response)   
        
        for thesis in node.xpath(self.itertag):
            record = HEPLoader(item=HEPRecord(), selector=thesis, response=response)  
            
            #Author, affiliation, year:
            author_line = thesis.xpath(
                "./div[@class = 'content clearfix']//div[@class='field-item even']"
                "/p[contains(text(),'Thesis')]/text()"
                ).extract()
            #author_line looks like this: 
            #[u'Chukman So, PhD Thesis, University of California, Berkeley (2014)']
            try:
                self.author_data = self.parse_author_data(author_line[0])
                authors = self.get_authors()
                record.add_value('authors', authors)
                record.add_value('date_published', self.author_data[0]['year'])
            except:
                print("Author data couldn't be found. "
                    "There's something wrong with the HTML structure, check the source")
                pass
            
            #Abstract:
            record.add_xpath('title', "./div[@class = 'node-headline clearfix']//a/text()")
            abs_paragraphs = thesis.xpath(
                "./div[@class = 'content clearfix']//div[@class='field-item even']"
                "/p[normalize-space()][string-length(text()) > 0][position() < last()]/text()"
                ).extract()
            try:
                abstract = self.get_abstract(abs_paragraphs)
                record.add_value("abstract", abstract)
            except:
                print("Abstract couldn't be found. "
                    "Theres's something wrong with the HTML structure, check the source")
                pass
            
            #PDF link:
            record.add_xpath('files', "./div[@class = 'content clearfix']//span[@class='file']/a/@href")
            #Experiment name:
            record.add_value('source', 'Alpha experiment')
            
            yield record.load_item()


    


