import schedule
from crawler.greenhousegas import GreenhouseGasCrawler


def execute(url_name: str, year: str, crawler_cls: str) -> None:
    crler = crawler_cls(url_name=url_name)
    crler.execute(year=year)


schedule.every().day.at("00:00").do(
    execute, url_name="greenhousegas", year="109", crawler_cls=GreenhouseGasCrawler
)
schedule.every().day.at("00:00").do(
    execute, url_name="greenhousegas", year="110", crawler_cls=GreenhouseGasCrawler
)
schedule.every().day.at("00:00").do(
    execute, url_name="greenhousegas", year="111", crawler_cls=GreenhouseGasCrawler
)
schedule.every().day.at("00:00").do(
    execute, url_name="greenhousegas", year="112", crawler_cls=GreenhouseGasCrawler
)


while True:
    schedule.run_pending()
    sleep(1)
