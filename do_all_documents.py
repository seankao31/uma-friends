import os
from pymongo import MongoClient, ASCENDING
from uma_friends.gamewith_normalizer import GamewithNormalizer, OutdatedError


UMAFRIENDS_DB_URI = os.environ['UMAFRIENDS_DB_URI']
UMAFRIENDS_DB = os.environ['UMAFRIENDS_DB']
RAW_GAMEWITH_FRIENDS_NS = os.environ['RAW_GAMEWITH_FRIENDS_NS']
UMA_FRIENDS_NS = os.environ['UMA_FRIENDS_NS']
FAILED_BUFFER_NS = os.environ['FAILED_BUFFER_NS']
GAME_DATA_DB = os.environ['GAME_DATA_DB']


def clean():
    mongo_client = MongoClient(UMAFRIENDS_DB_URI, tz_aware=True)
    uma_friends_db = mongo_client[UMAFRIENDS_DB]
    raw_friends = uma_friends_db[RAW_GAMEWITH_FRIENDS_NS]
    uma_friends = uma_friends_db[UMA_FRIENDS_NS]
    failed_collection = uma_friends_db[FAILED_BUFFER_NS]

    game_data_db = mongo_client[GAME_DATA_DB]
    gamewith_normalizer = GamewithNormalizer(game_data_db)

    total = raw_friends.count_documents({})
    i = 0
    for document in list(raw_friends.find().sort('post_date', ASCENDING)):
        try:
            friend_data = gamewith_normalizer.normalize(document)
            uma_friends.insert_one(friend_data)
        except OutdatedError:
            failed_collection.insert_one(document)
        i += 1
        print(f'{i}/{total}', end='\r')
    print(f'{i}/{total}')
    uma_friends.create_index(
        [('friend_code', ASCENDING), ('post_date', ASCENDING)],
        unique=True
    )
    failed_collection.create_index(
        [('friend_code', ASCENDING), ('post_date', ASCENDING)],
        unique=True
    )
    print('Finished.')


def limit_to_int():
    mongo_client = MongoClient(UMAFRIENDS_DB_URI, tz_aware=True)
    uma_friends_db = mongo_client[UMAFRIENDS_DB]
    uma_friends = uma_friends_db[UMA_FRIENDS_NS]
    total = uma_friends.count_documents({})
    i = 0
    for document in list(uma_friends.find()):
        try:
            friend_data = gamewith_normalizer.normalize(document)
            uma_friends.insert_one(friend_data)
        except OutdatedError:
            failed_collection.insert_one(document)
        i += 1
        print(f'{i}/{total}', end='\r')
    print(f'{i}/{total}')
    uma_friends.create_index(
        [('friend_code', ASCENDING), ('post_date', ASCENDING)],
        unique=True
    )
    failed_collection.create_index(
        [('friend_code', ASCENDING), ('post_date', ASCENDING)],
        unique=True
    )
    print('Finished.')


if __name__ == '__main__':
    limit_to_int()
