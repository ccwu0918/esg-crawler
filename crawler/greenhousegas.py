import pandas as pd
import arrow
from time import sleep
from pathlib import Path
import sys
import html
import re
import csv
import numpy as np
from tqdm import tqdm
import numpy as np

from base import BaseCrawler

sys.path.append(str(Path(__file__).absolute().parents[1]))

from utils.log_utils import logger

industryNames = dict()
with open('../data/coid_industry.csv', newline='') as f:
  datas = csv.DictReader(f, skipinitialspace=True)
  for row in datas:
    industryNames[row['CompCode']] = row['IndCat']

class GreenhouseGasCrawler(BaseCrawler):
    def __init__(self, url_name: str = "greenhousegas"):
        super().__init__(url_name=url_name)

    def get_page(self, url: str, data: dict):
        return self._send_requests(
            url=url, data=data, method="post", parser="html.parser"
        )

    def crawl_article_list(self, year: str):
        post_data = {
            "step": "1",
            "firstin": "1",
            "off": "1",
            "TYPEK": "sii",
            "year": year,
        }
        soup = self.get_page(url=self.urls["articleList"], data=post_data)
        co_ids = [
            item.text.strip()
            for item in soup.find_all("td") if not item.find("input")
        ]
        co_ids_names = {}
        for i in range(4, len(co_ids), 2):
          co_ids_names[co_ids[i]] = co_ids[i+1]  
        co_ids = [co_id for co_id in co_ids if co_id.isdigit()]        
        return co_ids, co_ids_names        

    def crawl_article(self, co_id: str, co_id_name: str, industry: str, year: str):
        post_data = {
            "step": "2",
            "co_id": co_id,
            "year": year,
            "TYPEK": "sii",
            "firstin": "true"
        }

        # post_data = {
        #     "step": "0",
        #     "firstin": "1",
        #     "off": "1",
        #     "TYPEK": "all",
        #     "keyword4": "",
        #     "code1": "",
        #     "TYPEK2": "",
        #     "checkbtn": "",
        #     "queryName": "co_id",
        #     "inpuType": "co_id",
        #     "co_id": co_id,
        #     "year": year
        # } 
        soup = self.get_page(url=self.urls["articleDetail"], data=post_data)
        _html_text = html.unescape(soup.text.strip())

        if (_html_text.find("年說明") > 0):
          _html_text = _html_text[_html_text.find("年說明")+4:]
        elif (_html_text.find("資料年度") > 0):
          _html_text = _html_text[_html_text.find("資料年度")+9:]
        
        pattern = "<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});|\n"
        _html_text = re.sub(pattern, "", _html_text).strip() 
        return {
            "co_id": co_id,
            "company_name": co_id_name,
            "content": _html_text,
            "datetime": f"{int(year) + 1911}-06-01 00:00:00",
            "year": year,
            "url": self.urls["articleDetail"],            
            "industry": industry
        }

    def execute(self, year: str = "111"):
        try:
            co_ids, co_ids_names = self.crawl_article_list(year=year)
            # co_ids_names = {}
            # co_ids = ['2017', '2022'] 
            # co_ids_names['2017'] = '官田鋼'            
            # co_ids_names['2022'] = '聚亨'
            result, failed_ids = [], []
            for co_id in tqdm(co_ids):
                try:
                    _res = self.crawl_article(co_id=co_id, co_id_name=co_ids_names[co_id], industry=industryNames[co_id], year=year)
                    _res = self.article_update_metadata(
                        identifier=f'{_res["year"]}-{_res["co_id"]}', **_res
                    )
                    result.append(_res)
                except Exception as err:
                    logger.error(err)
                    failed_ids.append({"url_name": self.url_name, "co_id": co_id})
                self.DEFAULT_WAIT_SECONDS = int(np.random.randint(3, 5))
                sleep(self.DEFAULT_WAIT_SECONDS)

            # result = pd.read_csv(f'../data/esg_employee_benefits-{year}_datas.csv', header=0)
            data = pd.DataFrame(result)
            # print(type(data))
            print(data.info())
            print(data.iloc[0])
            self.document_store(data)
            self.failed_urls_store(failed_ids)
            return data
        except Exception as err:
            logger.error(err)
            pass


if __name__ == "__main__":
    crawler = GreenhouseGasCrawler(url_name="esg") # "greenhousegas"
    data100 = crawler.execute(year="100")
    # crawler.execute(year="109")
    # crawler.execute(year="110")
    # crawler.execute(year="111")
    # crawler.execute(year="112")
