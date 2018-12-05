import logging
import schedule
import time
import sys
from typing import List

from data_crawler.common.config import logging_init
from data_crawler.common.config import DCConfig
from data_crawler.common.structure import get_log_folder
from data_crawler.crawlers import Crawler
from data_crawler.crawlers.waste.brno_separable_waste import BrnoSepWasteCrawler
from data_crawler.crawlers.waste.prague_separable_waste_containers import PragueSepWasteCrawler
from data_crawler.crawlers.waste.prague_waste_collection_points import PragueWasteCollectionPointsCrawler

crawlers: List[Crawler] = []


def run_init(conf_path):
    DCConfig.load(conf_path)
    get_log_folder()
    logging_init()
    logging.getLogger().setLevel(DCConfig.get_logging_level())
    # Init of crawlers
    crawlers.append(BrnoSepWasteCrawler())
    crawlers.append(PragueSepWasteCrawler())
    crawlers.append(PragueWasteCollectionPointsCrawler())


def run():
    logging.getLogger('dc').debug('Running scheduled task to crawl data sources.')
    for crawler in crawlers:
        crawler.run()


def schedule_tasks(schedule_opt):
    if schedule_opt == 'weekly':
        schedule.every().week.at('00:00').do(run)
    elif schedule_opt == 'daily':
        schedule.every().day.at('00:00').do(run)
    elif schedule_opt == 'hourly':
        schedule.every().hour.at().do(run)
    elif schedule_opt == 'test':
        schedule.every().second.do(run)


if __name__ == '__main__':
    conf_path = None
    if len(sys.argv) == 2:
        conf_path = sys.argv[1]
    run_init(conf_path)
    logging.getLogger('dc').info('Data pipeline has been started.')
    schedule_opt = DCConfig.get_schedule()
    schedule_tasks(schedule_opt)
    run()
    while True:
        schedule.run_pending()
        time.sleep(1)