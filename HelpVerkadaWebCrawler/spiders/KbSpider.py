import re
import csv
import os
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from HelpVerkadaWebCrawler.settings import languages

class KbSpider(CrawlSpider):
    
    name = "kbspider"
    allowed_domains = ["help.verkada.com"]
    start_urls = [f"https://help.verkada.com/{lang}" for lang in languages]

    handle_httpstatus_list = [404, 301]

    rules = (
        Rule(
            LinkExtractor(allow=[r".*/collections", r".*/articles"]),
            callback="parse_item",
            follow=True,
        ),
    )



    def parse_item(self, response):
        
        #pull list of all outgoing edges from the page
        edge_links = LinkExtractor(allow=[r".*/collections", r".*/articles"],
                                deny=[r'#']).extract_links(response)
        #pull list of all collection links from the page
        
        collection_links = LinkExtractor(allow=r".*/collections",
                                deny=[r'#']).extract_links(response)


        lang = response.url.split('/')[3]

        #check if the page is a 404 and log error if it is
        if response.status == 404:
            return
        
        if response.status == 301:
            redirect_url = response.headers.get('Location').decode('utf-8')
            if redirect_url:
                # Issue a new request to the redirect URL
                yield scrapy.Request(redirect_url, callback=self.parse_item)
            return
        
        yield {
            "Id":response.url,
            "Label":response.css("header::text").get(),
            "Language":f"{lang}",
        }



        for link in edge_links:
            if link.url != response.url:
                yield scrapy.Request(url=link.url, callback=self.parse_edges, meta={'referring_url': response.url,}, dont_filter=True)

        if re.match(r"https://help.verkada.com/.+/articles", response.url):
            for link in collection_links:
                if link.url != response.url:
                    yield scrapy.Request(url=link.url, callback=self.parse_collections, meta={'referring_url': response.url}, dont_filter=True)
                
    def parse_edges(self, response):    
        referring_url = response.meta['referring_url']
        
        if response.status in [404, 301]:
            self.bad_link_handler(response, referring_url)

        if response.status == 301:
            redirect_url = response.headers.get('Location').decode('utf-8')
            if redirect_url:
                # Issue a new request to the redirect URL
                yield scrapy.Request(redirect_url, callback=self.parse_edges,dont_filter=True, meta={'referring_url': referring_url})
            return
        
        yield {
            "Source": referring_url,
            "Target": response.url,
        }

    def parse_collections(self, response):
        referring_url = response.meta['referring_url']
        
        if response.status == 404:
            return

        if response.status == 301:
            redirect_url = response.headers.get('Location').decode('utf-8')
            if redirect_url:
                # Issue a new request to the redirect URL
                yield scrapy.Request(redirect_url, callback=self.parse_collections,dont_filter=True, meta={'referring_url': referring_url})
            return

        yield {
            "Id": referring_url,
            "Collection Names": response.css("header::text").get(),
            "Collection Links": response.url,
        }

    def bad_link_handler(self, response, referring_url):
        # Define the file paths
        not_found_file_path = './Output/pages_not_found.csv'
        redirects_file_path = './Output/permanent_redirects.csv'

        if response.status == 404:
            error_data = [response.url, referring_url, "404 Not Found"]
            self.log_and_write_csv(not_found_file_path, error_data, ["URL", "Referring URL", "Status"])

        elif response.status == 301:
            error_data = [response.url, referring_url, "301 Moved Permanently"]
            self.log_and_write_csv(redirects_file_path, error_data, ["URL", "Referring URL", "Status"])

    def log_and_write_csv(self, file_path, data, header):
        # Log the error to the console
        self.logger.warning(", ".join(data))

        # Check if the file exists already
        file_exists = os.path.isfile(file_path)

        # Open the CSV file and append the data
        with open(file_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # If the file does not exist, write the header first
            if not file_exists:
                writer.writerow(header)

            writer.writerow(data)


    