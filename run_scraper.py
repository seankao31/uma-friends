import os
from pymongo import MongoClient
from selenium import webdriver

from uma_friends.gamewith_normalizer import GamewithNormalizer
from uma_friends.gamewith_scraper import GamewithScraper
from uma_friends.utils import get_logger


logger = get_logger()


GOOGLE_CHROME_BIN = os.environ['GOOGLE_CHROME_BIN']
CHROMEDRIVER_PATH = os.environ['CHROMEDRIVER_PATH']

BUTTON_LIMIT = int(os.environ['BUTTON_LIMIT'])

UMAFRIENDS_DB_URI = os.environ['UMAFRIENDS_DB_URI']
UMAFRIENDS_DB = os.environ['UMAFRIENDS_DB']
RAW_GAMEWITH_FRIENDS_NS = os.environ['RAW_GAMEWITH_FRIENDS_NS']
UMA_FRIENDS_NS = os.environ['UMA_FRIENDS_NS']
FAILED_BUFFER_NS = os.environ['FAILED_BUFFER_NS']
GAME_DATA_DB = os.environ['GAME_DATA_DB']

GAMEWITH_FRIENDS_URL = os.environ['GAMEWITH_FRIENDS_URL']


def run_scraper():
    chrome_option = webdriver.ChromeOptions()
    chrome_option.binary_location = GOOGLE_CHROME_BIN
    chrome_option.add_argument('--headless')
    chrome_option.add_argument('--no-sandbox')
    chrome_option.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, options=chrome_option)

    mongo_client = MongoClient(UMAFRIENDS_DB_URI, tz_aware=True)
    uma_friends_db = mongo_client[UMAFRIENDS_DB]
    raw_collection = uma_friends_db[RAW_GAMEWITH_FRIENDS_NS]
    clean_collection = uma_friends_db[UMA_FRIENDS_NS]
    failed_collection = uma_friends_db[FAILED_BUFFER_NS]
    game_data_db = mongo_client[GAME_DATA_DB]

    gamewith_normalizer = GamewithNormalizer(game_data_db)

    gamewith_scraper = GamewithScraper(driver=driver,
                                       url=GAMEWITH_FRIENDS_URL,
                                       timeout=30,
                                       button_limit=BUTTON_LIMIT,
                                       raw_collection=raw_collection,
                                       clean_collection=clean_collection,
                                       failed_collection=failed_collection,
                                       gamewith_normalizer=gamewith_normalizer)
    gamewith_scraper.run()


if __name__ == '__main__':
    run_scraper()
