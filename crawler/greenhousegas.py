import pandas as pd
import arrow
from time import sleep
from pathlib import Path
import sys
from tqdm import tqdm

from base import BaseCrawler

sys.path.append(str(Path(__file__).absolute().parents[1]))

from utils.log_utils import logger


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
        co_ids = [co_id for co_id in co_ids if co_id.isdigit()]
        return co_ids

    def crawl_article(self, co_id: str, year: str):
        post_data = {
            "step": "2",
            "co_id": co_id,
            "year": year,
            "TYPEK": "sii",
            "firstin": "true"
        }
        soup = self.get_page(url=self.urls["articleDetail"], data=post_data)
        return {
            "co_id": co_id,
            "year": year,
            "url": self.urls["articleDetail"],
            "content": soup.text.strip(),
            "datetime": f"{int(year) + 1911}-06-01 00:00:00"
        }

    def execute(self, year: str = "111"):
        try:
            co_ids = self.crawl_article_list(year=year)
            result, failed_ids = [], []
            for co_id in tqdm(co_ids):
                try:
                    _res = self.crawl_article(co_id=co_id, year=year)
                    _res = self.article_update_metadata(
                        identifier=f'{_res["year"]}-{_res["co_id"]}', **_res
                    )
                    result.append(_res)
                except Exception as err:
                    logger.error(err)
                    failed_ids.append({"url_name": self.url_name, "co_id": co_id})
                
                sleep(self.DEFAULT_WAIT_SECONDS)

            data = pd.DataFrame(result)
            print(data.info())
            print(data.iloc[0])
            self.document_store(data)
            self.failed_urls_store(failed_ids)

        except Exception as err:
            logger.error(err)
            pass


if __name__ == "__main__":
    crawler = GreenhouseGasCrawler(url_name="greenhousegas")
    crawler.execute(year="100")
    #crawler.execute(year="109")
    #crawler.execute(year="110")
    #crawler.execute(year="111")
    #crawler.execute(year="112")
