import requests
import pandas as pd
import numpy as np
from lxml import etree, html
import xmltodict
import json
from datetime import datetime
from tqdm import tqdm


class crawl_ohou():
    base_size = 100

    base_advices_url = "https://ohou.se/advices/"
    base_projects_url = "https://ohou.se/projects/"
    base_feeds_url = "https://ohou.se/cards/feed/"    

    base_advices_list_url = "https://ohou.se/advices.json"
    base_projects_list_url = "https://ohou.se/projects.json"
    base_feeds_list_url = "https://ohou.se/cards/feed.json"

    headers = {"user-agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}
    
    def __init__(self):
        pass


    def create_params(self, query, page = 1, size = 1):
        params= {"query" : query,
                 "input_source" : "advices",
                 "page" : page,
                 "per" : size,
                 "v" : 5}
        
        return params

    
    def get_base_url(self, type):
        if type == "feeds":
            base_url = self.base_feeds_list_url
        elif type == "projects":
            base_url = self.base_projects_list_url
        elif type == "advices":
            base_url = self.base_advices_list_url

        return base_url


    def create_content_url(self, content_id, type):
        if type == "feeds":
            content_url = self.base_feeds_url + str(content_id)
        elif type == "projects":
            content_url = self.base_projects_url + str(content_id)
        elif type == "advices":
            content_url = self.base_advices_url + str(content_id)

        return content_url



    def get_total_count(self, query, type):
        params = self.create_params(query = query, page = 1, size = 1)

        base_url = self.get_base_url(type = type)

        rq = requests.get(base_url, params = params, headers = self.headers, verify=False)
        rq_json = rq.json()
        total_count = rq_json["total_count"]

        return total_count

    
    def request_contents_urls(self, query, type, num_request = None):
        """
            type: ("feeds", "advices", "projects")
        """
        total_count = self.get_total_count(query = query, type = type)
        print(f"query : {query}, total_count : {total_count}, num_request : {num_request}")
        base_url = self.get_base_url(type = type)

        if num_request is not None:
            num_request = min(num_request, total_count)
        else:
            num_request = total_count


        max_page = num_request // self.base_size
        remainder = num_request % self.base_size


        contents_urls = []
        contents_datetimes = []
        for pg in range(1, max_page+2):
            if pg < max_page+1:
                request_size = self.base_size
            else:
                request_size = remainder
            params = self.create_params(query = query, page = pg, size = request_size)
            rq = requests.get(base_url, params = params, headers = self.headers, verify = False)
            self.rq= rq
            contents_ids = [x["id"] for x in rq.json()[type]]
            contents_datetime = [x["created_at"] for x in rq.json()[type]]
            contents_datetimes.extend(contents_datetime)
            for cid in contents_ids:
                content_url = self.create_content_url(content_id = cid, type = type)
                contents_urls.append(content_url)
        
        return contents_urls, contents_datetimes
    

    def extract_content_text(self, rq):
        hp = etree.HTMLParser(encoding='utf-8')
        tree = html.fromstring(rq.content, parser= hp)

        contents_dom = tree.find(".//div[@class='bpd-view content-detail__content-bpd advice-detail__content-detail']")
        bpd_view_text_dom = tree.xpath(".//*[re:match(@class, 'bpd-view-text')]", namespaces={"re": "http://exslt.org/regular-expressions"})
        bpd_view_text_list = [x.xpath(".//text()") for x in bpd_view_text_dom]
        bpd_view_text_list_flatten = [str(y).replace("\r", "") for x in bpd_view_text_list for y in x]

        content = "\n".join(bpd_view_text_list_flatten)

        return content

    
    def extract_content_keywords(self, rq): 
        hp = etree.HTMLParser(encoding='utf-8')
        tree = html.fromstring(rq.content, parser= hp)
        keywords = tree.xpath(".//li[@class='content-keyword-list__item']//text()")
        keywords = [k for k in keywords if k != "#"]
        if len(keywords) > 0:
            keywords_list = [str(x).replace("\r", "") for x in keywords]
        else:
            keywords_list = None

        return keywords_list


    def request_content(self, content_url):
        rq = requests.get(content_url, headers = self.headers, verify=False)

        contents = self.extract_content_text(rq = rq)
        keywords = self.extract_content_keywords(rq = rq)
        

        return {"contents" : contents, "keywords" : keywords}

            
    def request_contents(self, contents_urls):
        contents_list = []
        for content_url in tqdm(contents_urls):
            content = self.request_content(content_url = content_url)
            contents_list.append(content)

        return contents_list

    
    def request_feeds(self, query, num_request, type):
        total_count = self.get_total_count(query = query, type = type)
        print(f"query : {query}, total_count : {total_count}, num_request : {num_request}")
        base_url = self.get_base_url(type = type)

        if num_request is not None:
            num_request = min(num_request, total_count)
        else:
            num_request = total_count


        max_page = num_request // self.base_size
        remainder = num_request % self.base_size


        contents_list = []
        for pg in range(1, max_page+2):
            if pg < max_page+1:
                request_size = self.base_size
            else:
                request_size = remainder
            params = self.create_params(query = query, page = pg, size = request_size)
            rq = requests.get(base_url, params = params, headers = self.headers, verify = False)
            
            self.check = rq.json()["cards"]
            for x in rq.json()["cards"]:
                qr_result = {"id" : x["id"],
                             "description" : x["description"],
                             "keywords" : x["keywords"],
                             "datetime" : x.get("created_at")}
                contents_list.append(qr_result)

        return contents_list


    def run(self, query, type, num_request = None):
        if isinstance(query, list):
            query = [str(x) for x in query]
        else:
            query = [str(query)]

        crawl_result = []
        
        for qr in query:
            start = datetime.now()
            if type in ["advices", "projects"]:
                contents_urls, contents_datetime = self.request_contents_urls(query = qr, type = type, num_request = num_request)
                print(f"contents_urls : {len(contents_urls)}, contents_datetime : {len(contents_datetime)}")
                self.contents_urls = contents_urls
                self.contents_datetime = contents_datetime
                print(f"query : {qr}, contents_urls : {len(contents_urls)}")
                
                contents_list = self.request_contents(contents_urls = contents_urls)

                for i in range(len(contents_list)):
                    qr_result = {"query" : qr,
                                "type" : type,
                                "url" : contents_urls[i],
                                "contents" : contents_list[i]["contents"],
                                "keywords" : contents_list[i]["keywords"],
                                "datetime" : contents_datetime[i]}
                    crawl_result.append(qr_result)
            elif type == "feeds":
                contents_list = self.request_feeds(query = qr, type = type, num_request = num_request)
                for i in range(len(contents_list)):
                    qr_result = {"query" : qr,
                                "type" : type,
                                "contents" : contents_list[i]["description"],
                                "keywords" : contents_list[i]["keywords"],
                                "datetime" : contents_list[i]["datetime"]}
                    crawl_result.append(qr_result)
                print(f"Elapsed time : {datetime.now() - start}")
        return crawl_result

