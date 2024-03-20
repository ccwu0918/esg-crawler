import arrow
from abc import abstractmethod
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import os
import time
import urllib.parse
import sys
import numpy as np

sys.path.append(str(Path(__file__).absolute().parents[1]))

from utils.qdrant_utils import QdrantDBWrapper
from utils.data_utils import read_data, save_data, hash_string_to_ID
from utils.log_utils import logger
from utils.curation_utils import to_embeddings, to_openai_embeddings


class BaseCrawler:
    ROOT_DIR = Path(__file__).absolute().parents[1]
    DATA_DIR = f"{ROOT_DIR}/data"
    SITE_CONFIG = read_data(path=f"{ROOT_DIR}/config.yaml")
    ua = UserAgent()
    DEFAULT_TIMEZONE = "Asia/Taipei"
    DEFAULT_COLLECTION_NAME = "esg"
    DEFAULT_RETRY = 3
    DEFAULT_WAIT_SECONDS = 1

    def __init__(self, url_name):
        self.url_name = url_name
        self.DEFAULT_COLLECTION_NAME = url_name
        self.urls = self.SITE_CONFIG["WEBSITE"][self.url_name]

    def _get_domain(self, url: str):
        return f"https://{urllib.parse.urlparse(url).netloc}"

    def _send_requests(self, url: str, **kwargs):
        soup = None
        retry = 0
        parser = "html.parser" if "parser" not in kwargs else kwargs["parser"]
        method = "get" if "method" not in kwargs else kwargs["method"]
        if "headers" not in kwargs:
            kwargs["headers"] = {"user-agent": self.ua.google}
        param_kwargs = {k: v for k, v in kwargs.items() if k not in ["method", "parser"]}
        while soup is None and retry < self.DEFAULT_RETRY:
            if method == "get":
                res = requests.get(url, **param_kwargs).content
            elif method == "post":
                res = requests.post(url, **param_kwargs).content
            soup = BeautifulSoup(res, parser)
            self.DEFAULT_WAIT_SECONDS = int(np.random.randint(3, 5))
            time.sleep(self.DEFAULT_WAIT_SECONDS)
            retry += 1
        return soup
    
    def get_page(self, url: str):
        logger.info(f"crawling {url} ...")
        return self._send_requests(url=url)

    @abstractmethod
    def crawl_article_list(self, **kwargs):
        pass

    @abstractmethod
    def crawl_article(self, **kwargs):
        pass

    def article_update_metadata(self, identifier: str, **kwargs):
        idx = hash_string_to_ID(identifier) if "idx" not in kwargs else kwargs["idx"]
        kwargs.update(
            {
                "idx": int(idx), "url_name": self.url_name,
                "timestamp": int(arrow.get(kwargs["datetime"]).timestamp()),
                "updated_at": arrow.now(self.DEFAULT_TIMEZONE).format("YYYY-MM-DD HH:mm:ss"),
            }
        )
        return kwargs

    def document_store(self, data):
        esg_datas = pd.DataFrame(data)      
        save_data(data=esg_datas, path=f"{self.DATA_DIR}/{self.url_name}_datas.csv")
        years = data["year"][0]                        
        save_data(data=esg_datas, path=f"{self.DATA_DIR}/{self.url_name}-{years}_datas.csv")        
        datas = {}
        datas["full_text"] = data.apply(
            # lambda row: f'{row["year"]}\n{row["co_id"]}\n{row["content"]}', axis=1
            lambda row: f'{row["year"]}\n{row["co_id"]}\n{row["company_name"]}\n{row["industry"]}\n{row["content"]}', axis=1
        )
        logger.info(f'storing {data} ...')
        # to_embeddings
        embeddings = to_openai_embeddings(datas["full_text"].tolist())
        qdrant = QdrantDBWrapper()
        qdrant.upsert(
            collection_name=self.DEFAULT_COLLECTION_NAME, data=data, embeddings=embeddings,
        )
        

    def failed_urls_store(self, new_failed_urls: list):
        path = f"{self.DATA_DIR}/failed_urls.csv"
        if os.path.exists(path):
            origin_failed_urls = read_data(path=path)
            failed_urls = pd.concat(
                [origin_failed_urls, pd.DataFrame(new_failed_urls)], axis=0
            ).drop_duplicates(subset=["url"])
        else:
            failed_urls = pd.DataFrame(new_failed_urls)
        if not failed_urls.empty:
            save_data(data=failed_urls, path=f"{self.DATA_DIR}/failed_urls.csv")

    def execute(self, **kwargs):
        try:
            news_list = self.crawl_article_list(url=url)
            news_result, failed_urls = [], []
            for url in tqdm(news_list):
                try:
                    _res = self.crawl_article(url=url)
                    _res = self.article_update_metadata(**_res)
                    news_result.append(_res)
                except Exception as err:
                    logger.error(err)
                    failed_urls.append({"url_name": self.url_name, "url": url})

            data = pd.DataFrame(news_result)
            print(data.info())
            print(data.iloc[0])
            self.document_store(data)
            self.failed_urls_store(failed_urls)
        
        except Exception as err:
            logger.error(err)
            pass
