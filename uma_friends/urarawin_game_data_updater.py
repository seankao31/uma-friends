import json
import logging

from bs4 import BeautifulSoup
import requests


logger = logging.getLogger(__name__)


class DataError(Exception):
    pass


class UrarawinGameDataUpdater:
    '''Updates game database using data collected by urarawin website.'''
    def __init__(self, urarawin_db_url, uma_article_base_url, game_data_database):
        '''Initializes UrarawinGameDataUpdater.

        Attributes:
            urarawin_db_url:
                A string of url link to the urarawin github game db json file.
            uma_article_base_url:
                A string of base url link of gamewith articles.
            game_data_database:
                A pymongo database.
        '''
        self._urarawin_db_url = urarawin_db_url
        self._uma_article_base_url = uma_article_base_url
        if self._uma_article_base_url[-1] != '/':
            self._uma_article_base_url += '/'
        self._game_data_database = game_data_database
        self._COLLECTION_NAMES = [
            'players',
            'supports',
            'skills',
            'races',
            'buffs',
            'effects',
            'events'
        ]
        logger.info('Finished initializing UrarawinGameDataUpdater.')

    def run(self):
        logger.info('Started running UrarawinGameDataUpdater.')
        game_data = self._download_game_data()
        self._preprocess_game_data(game_data)
        self._write_to_database(game_data)
        logger.info('Finished running UrarawinGameDataUpdater.')

    def _download_game_data(self):
        '''Downloads game data.

        Returns:
            Json (dict) of game data.

        Raises:
            DataError:
                If game data lacks some keys.
        '''
        logger.info('Started downloading game data. %s',
                    json.dumps({'url': self._urarawin_db_url}, ensure_ascii=False))
        game_data = requests.get(self._urarawin_db_url).json()
        if not self._validate(game_data):
            raise DataError('Failed validating game data.')
        logger.info('Finished downloading game data. %s',
                    json.dumps({'url': self._urarawin_db_url}, ensure_ascii=False))
        return game_data

    def _validate(self, game_data):
        '''Very basic validation. Not meant to be comprehensive.

        Args:
            game_data:
                A dict.

        Returns:
            Bool whether the game_data has expected fields.
        '''
        return all(name in game_data for name in self._COLLECTION_NAMES)

    def _preprocess_game_data(self, game_data):
        '''Preprocess game data.

        1. Changes game_data['effects] to a list of dict.
        2. Adds gwImgUrl field to players (uma) documents, mapping each uma
        to the image url of gamewith site.

        Args:
            game_data:
                A dict. It will be modified.
        '''
        logger.info('Started preprocessing game data.')

        effects = game_data['effects']
        new_effects = []
        for k, v in effects.items():
            v['id'] = k
            new_effects.append(v)
        game_data['effects'] = new_effects

        logger.info('Started mapping uma gamewith image url.')
        for uma in game_data['players']:
            gamewith_id = uma['gwId']
            url = self._uma_article_base_url + gamewith_id
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'lxml')
            a = soup.find('a', href=url)
            gwImgUrl = a.img['data-original']
            uma['gwImgUrl'] = gwImgUrl

        logger.info('Finished preprocessing game data.')

    def _write_to_database(self, game_data):
        '''Writes game data to database.

        Args:
            game_data:
                A dict.
        '''
        for collection_name in self._COLLECTION_NAMES:
            collection = self._game_data_database[collection_name]
            logger.info('Started inserting data into collection. %s',
                        json.dumps({'collection': collection.full_name}))
            documents = game_data[collection_name]
            collection.drop()
            logger.info('Collection dropped. %s',
                        json.dumps({'collection': collection.full_name}))
            collection.insert_many(documents)
            logger.info('Finished inserting data into collection. %s',
                        json.dumps({'collection': collection.full_name}))

        self._game_data_database['skills'].create_index('name')
        self._game_data_database['players'].create_index('uniqueSkillList')
        logger.info('Finsihed creating index in game database.')
