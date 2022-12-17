from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from scrapy.utils.log import configure_logging
from twisted.internet import reactor, defer
from scrapy import spiderloader
import sys
import logging

from labeb.spiders.carrefour_ksa import CarrefourKSA
from labeb.spiders.carrefour_jordan import CarrefourJordan
from labeb.spiders.carrefour_kuwait import CarrefourKuwait
from labeb.spiders.carrefour_qatar import CarrefourQatar
from labeb.spiders.carrefour_uae import CarrefourUAE
from labeb.spiders.lulu_bahrain import LuluBahrain
from labeb.spiders.lulu_egypt import LuluEgypt
from labeb.spiders.lulu_ksa import LuluKsa
from labeb.spiders.lulu_kuwait import LuluKuwait
from labeb.spiders.lulu_oman import LuluOman
from labeb.spiders.lulu_qatar import LuluQatar
from labeb.spiders.lulu_uae import LuluUae

import asyncio

from post_script import post_main


def main():
    configure_logging()
    settings = get_project_settings()
    runner = CrawlerRunner(settings)

    @defer.inlineCallbacks
    def crawl():
        # configure_logging()
        # runner = CrawlerRunner()
        # settings = get_project_settings()
        # spider_loader = spiderloader.SpiderLoader.from_settings(settings)
        # spiders = spider_loader.list()
        # classes = [spider_loader.load(name) for name in spiders]
        # for my_spider, spider_name in zip(classes, spiders):
        #     print(f"Running spider: {spider_name}")
        #     yield runner.crawl(my_spider)
        print("Running spider CarrefourUAE")
        yield runner.crawl(CarrefourUAE)
        print("Running spider LuluUae")
        yield runner.crawl(LuluUae)
        reactor.stop()

    crawl()
    reactor.run()


if __name__ == "__main__":
    main()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(post_main())
