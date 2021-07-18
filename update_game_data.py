import os
from pymongo import MongoClient

from uma_friends.urarawin_game_data_updater import UrarawinGameDataUpdater
from uma_friends.utils import get_logger


logger = get_logger()


UMAFRIENDS_DB_URI = os.environ['UMAFRIENDS_DB_URI']
GAME_DATA_DB = os.environ['GAME_DATA_DB']

URARAWIN_DB_URL = os.environ['URARAWIN_DB_URL']
UMA_ARTICLE_BASE_URL = os.environ['UMA_ARTICLE_BASE_URL']


def run_updater():
    mongo_client = MongoClient(UMAFRIENDS_DB_URI)
    game_data_db = mongo_client[GAME_DATA_DB]

    urarawin_game_data_updater = UrarawinGameDataUpdater(
        urarawin_db_url=URARAWIN_DB_URL,
        uma_article_base_url=UMA_ARTICLE_BASE_URL,
        game_data_database=game_data_db
    )
    urarawin_game_data_updater.run()


if __name__ == '__main__':
    run_updater()
