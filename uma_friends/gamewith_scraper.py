import json
import logging
import re
import time

from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException
from pymongo import ASCENDING
from pymongo.errors import BulkWriteError

from .gamewith_normalizer import OutdatedError
from .utils import get_utc_datetime


logger = logging.getLogger(__name__)


DUPLICATE_KEY_ERROR_CODE = 11000


class PageError(Exception):
    pass


class GamewithScraper:
    '''A web scraper that fetches friend data from gamewith website.'''
    def __init__(self, driver, url, timeout, button_limit, raw_collection,
                 clean_collection, failed_collection, gamewith_normalizer):
        '''Initializes GamewithScraper.

        Args:
            driver:
                A selenium webdriver.
            URL:
                A string of url link to the gamewith uma musume
                friends sharing page.
            TIMEOUT:
                An integer indicating in seconds how long the scraper
                should keep trying to access some web elements.
            BUTTON_LIMIT:
                An integer of how many times at most should the
                "もっと見る" button be clicked.
            raw_collection:
                A pymongo Collection. Stores raw data scraped from gamewith.
            clean_collection:
                A pymongo Collection. Stores clean-up data.
            failed_collection:
                A pymongo Collection. Stores friend data that cannot be normalized.
            gamewith_normalizer:
                A GamewithNormalizer. Parses raw gamewith data.
        '''
        self._driver = driver
        self._URL = url
        self._TIMEOUT = timeout
        self._BUTTON_LIMIT = button_limit
        self._raw_collection = raw_collection
        self._clean_collection = clean_collection
        self._failed_collection = failed_collection
        self._gamewith_normalizer = gamewith_normalizer
        logger.info('Finished initializing GamewithScraper.')

    def run(self):
        '''Scrapes and stores data.'''
        logger.info('Started running GamewithScraper.')
        self._fix_failed_data()
        raw_friends_html = self._scrape_raw()
        friend_html_list = self._parse_friend_html_list(raw_friends_html)
        friends_data = self._get_friends_data(friend_html_list)
        self._insert_into_raw_database(friends_data)
        cleaned_data_list, failed_data_list = self._clean_data(friends_data)
        self._insert_into_clean_database(cleaned_data_list)
        self._insert_into_failed_database(failed_data_list)
        logger.info('Finished running GamewithScraper.')

    def _fix_failed_data(self):
        '''Attempts to fix friend data previously failed cleaning.'''
        logger.info('Started fixing failed data.')
        failed_friends_data = list(self._failed_collection.find())
        try:
            self._failed_collection.drop()
            logger.info('Dropped failed database. %s',
                        json.dumps({'collection': self._failed_collection.full_name}))
            cleaned_data_list, failed_data_list = self._clean_data(failed_friends_data)
            self._insert_into_clean_database(cleaned_data_list)
            self._insert_into_failed_database(failed_data_list)
        except:
            # Stopped cleaning failed data prematurely.
            # Insert failed data back into failed database (we just dropped it)
            # before raising the same exception again.
            self._insert_into_failed_database(failed_friends_data)
            logger.exception('Failed fixing failed data. Inserted failed data back into failed database. %s',
                             json.dumps({'collection': self._failed_collection.full_name}))
            raise
        logger.info('Finished fixing failed data.')

    def _parse_friend_html_list(self, raw_friends_html):
        '''Parse friends section html and find list of friend html.

        Args:
            raw_friends_html:
                A string of the friends section html.

        Returns:
            List of <li> elements.
        '''
        logger.info('Started parsing friend html list.')
        soup = BeautifulSoup(raw_friends_html, 'lxml')
        friend_html_list = soup.find_all(class_='-r-uma-musume-friends-list-item')

        if not friend_html_list:
            logger.error('Failed to parse friend html list.')
            logger.debug(json.dumps({'soup': soup}, ensure_ascii=False))
            # TODO: raise exception
        logger.info('Finished parsing friend html list. %s',
                    json.dumps({'count': len(friend_html_list)}))
        return friend_html_list

    def _get_friends_data(self, friend_html_list):
        '''Extract friends data from parsed friend html list.

        Args:
            friend_html_list:
                List of <li> elements.

        Returns:
            List of dicts consisting of friends data.
        '''
        logger.info('Started extracting friends data.')
        friends_data = []
        for friend_html in friend_html_list:
            friend_data = self._get_friend_data(friend_html)
            friends_data.append(friend_data)
        logger.info('Finished extracting friends data.')
        return friends_data

    def _get_friend_data(self, friend_html):
        '''Extract friend data from parsed friend html.

        Args:
            friend_html:
                A <li> element.

        Returns:
            A dict consisting of friend data. For example:

            {'friend_code': '248605600',
             'support_id': '262813',
             'support_limit': '4凸',
             'character_image_url': 'https://img.gamewith.jp/article_tools/uma-musume/gacha/i_25.png',
             'factors': [
                 'パワー3(代表3)',
                 'スタミナ6',
                 '差し2(代表2)',
                 'マイル4',
                 'Pride of KING1(代表1)',
                 '紅焔ギア/LP1211-M1',
                 'Shadow Break1',
                 '集中力1(代表1)',
                 '末脚3',
                 'URAシナリオ6(代表3)'
             ],
             'comment': 'スタミナ6\nパワー3\nマイル4\n差し2\nURA5\n\nキャンサー杯用に良かったら使って下さい。\n白因子省略\n代表URA☆3\n親2URA☆2'
             'post_date': ISODate('2021-07-15T10:09:00Z')}


             Values may be None if corresponding data isn't found.
        '''
        support_id = None
        support_wrap = friend_html.find_all(class_='-r-uma-musume-friends-list-item__support-wrap')
        if support_wrap:
            # Example href: 'https://gamewith.jp/uma-musume/article/show/262813'
            support_id = support_wrap[0].find_all('a')[0].get('href')
            support_id = support_id.split('/')[-1]

        support_limit = None
        support_limit_wrap = friend_html.find_all(class_='-r-uma-musume-friends-list-item__limitNumber')
        if support_limit_wrap:
            support_limit = support_limit_wrap[0].text.strip()

        trainer_id = None
        trainer_id_wrap = friend_html.find_all(class_='-r-uma-musume-friends-list-item__trainerId__text')
        if trainer_id_wrap:
            trainer_id = trainer_id_wrap[0].text.strip()

        main_uma_img = None
        main_uma_wrap = friend_html.find_all(class_='-r-uma-musume-friends-list-item__mainUmaMusume-wrap')
        if main_uma_wrap:
            main_uma_img = main_uma_wrap[0].img.get('src').strip()
            if main_uma_img == 'https://img.gamewith.jp/article_tools/uma-musume/gacha/i_undefined.png':
                main_uma_img = None

        factors = None
        factors_item = friend_html.find_all(class_='-r-uma-musume-friends-list-item__factor-list__item')
        if factors_item:
            factors = [factor.text.strip() for factor in factors_item]

        comment = None
        comment_wrap = friend_html.find_all(class_='-r-uma-musume-friends-list-item__comment')
        if comment_wrap:
            comment = comment_wrap[0].text.strip()

        post_date = None
        post_date_wrap = friend_html.find_all(class_='-r-uma-musume-friends-list-item__postDate')
        if post_date_wrap:
            post_date = post_date_wrap[0].text.strip()
            post_date = get_utc_datetime(post_date, '%m/%d %H:%M')

        friend_data = {
            'friend_code': trainer_id,
            'support_id': support_id,
            'support_limit': support_limit,
            'character_image_url': main_uma_img,
            'factors': factors,
            'comment': comment,
        }
        friend_data['post_date'] = post_date

        return friend_data

    def _insert_into_raw_database(self, friends_data):
        '''Insert friends data into raw database.

        Args:
            friends_data:
                List of dicts consisting of friends data.

        Raises:
            All exceptions raised by MongoClient,
            except for DuplicateKeyError.
        '''
        logger.info('Started inserting friends data into raw database. %s',
                    json.dumps({'collection': self._raw_collection.full_name}))
        try:
            insert_result = self._raw_collection.insert_many(friends_data, ordered=False)
        except BulkWriteError as e:
            # Ignore DuplicateKeyError
            n_error = len(e.details['writeErrors'])
            panic_list = [e_ for e_ in e.details['writeErrors']
                          if e_['code'] != DUPLICATE_KEY_ERROR_CODE]
            e.details['writeErrors'] = panic_list
            logger.info('Ignored duplications. %s',
                        json.dumps({'n_duplicate': n_error - len(panic_list)}))
            logger.info('Finished inserting friends data into raw database. %s',
                        json.dumps({'collection': self._raw_collection.full_name,
                                    'n_inserted': e.details['nInserted']}))
            if panic_list:
                logger.exception('Exception occurred during insertion.',
                                 exc_info=e, stack_info=True)
                raise e
        else:
            logger.info('Finished inserting friends data into raw database. %s',
                        json.dumps({'collection': self._raw_collection.full_name,
                                    'n_inserted': len(insert_result.inserted_ids)}))

        self._raw_collection.create_index(
            [('friend_code', ASCENDING), ('post_date', ASCENDING)],
            unique=True
        )
        logger.info('Finsihed creating index in raw database.')

    def _clean_data(self, friends_data):
        '''Parse raw friends data.

        Args:
            friends_data:
                List of dicts consisting of friends data.

        Returns:
            (cleaned_data_list, failed_data_list)
            cleaned_data_list: data that are parsed successfully.
            failed_data_list: otherwise.
        '''
        logger.info('Started cleaning friends data.')

        # Stores normalized friend data
        cleaned_data_list = []
        # Stores friend data (in original form) which failed to be normalize
        failed_data_list = []

        for friend_data in friends_data:
            try:
                cleaned_data = self._gamewith_normalizer.normalize(friend_data)
            except OutdatedError as e:
                friend_data_identify = {
                    'friend_code': friend_data['friend_code'],
                    'post_date': str(friend_data['post_date'])
                }
                logger.exception('Game database outdated. Lookup in game database failed. %s',
                                 json.dumps({'friend_data': friend_data_identify}, ensure_ascii=False),
                                 exc_info=e,
                                 stack_info=True)
                failed_data_list.append(friend_data)
                continue
            except Exception as e:
                friend_data_identify = {
                    'friend_code': friend_data['friend_code'],
                    'post_date': str(friend_data['post_date'])
                }
                logger.exception('Something went wrong during normalizing friend data. %s',
                                 json.dumps({'friend_data': friend_data_identify}, ensure_ascii=False),
                                 exc_info=e,
                                 stack_info=True)
                failed_data_list.append(friend_data)
                continue
            cleaned_data_list.append(cleaned_data)

        logger.info('Finished cleaning friends data.')
        return cleaned_data_list, failed_data_list

    def _insert_into_clean_database(self, cleaned_data_list):
        '''Insert cleaned data into clean database.

        Args:
            cleaned_data_list: List of parsed friends data.

        Raises:
            All exceptions raised by MongoClient,
            except for DuplicateKeyError.
        '''
        if cleaned_data_list:
            logger.info('Started inserting cleaned data into clean database. %s',
                        json.dumps({'collection': self._clean_collection.full_name}))
            try:
                insert_result = self._clean_collection.insert_many(cleaned_data_list, ordered=False)
            except BulkWriteError as e:
                # Ignore DuplicateKeyError
                n_error = len(e.details['writeErrors'])
                panic_list = [e_ for e_ in e.details['writeErrors']
                              if e_['code'] != DUPLICATE_KEY_ERROR_CODE]
                e.details['writeErrors'] = panic_list
                logger.info('Ignored duplications. %s',
                            json.dumps({'n_duplicate': n_error - len(panic_list)}))
                logger.info('Finished inserting cleaned data into clean database. %s',
                            json.dumps({'collection': self._clean_collection.full_name,
                                        'n_inserted': e.details['nInserted']}))
                if panic_list:
                    logger.exception('Exception occurred during insertion.',
                                     exc_info=e, stack_info=True)
                    raise e
            else:
                logger.info('Finished inserting cleaned data into clean database. %s',
                            json.dumps({'collection': self._clean_collection.full_name,
                                        'n_inserted': len(insert_result.inserted_ids)}))
            self._clean_collection.create_index(
                [('friend_code', ASCENDING), ('post_date', ASCENDING)],
                unique=True
            )
            self._clean_collection.create_index([('main_uma.id', ASCENDING), ('support.id', ASCENDING)])
            self._clean_collection.create_index([
                ('main_uma.factors.name', ASCENDING),
                ('main_uma.factors.type', ASCENDING),
                ('main_uma.factors.level', ASCENDING),
                ('main_uma.id', ASCENDING),
                ('support.id', ASCENDING)
            ])
            self._clean_collection.create_index([
                ('factors.name', ASCENDING),
                ('factors.type', ASCENDING),
                ('factors.level', ASCENDING),
                ('main_uma.id', ASCENDING),
                ('support.id', ASCENDING)
            ])

    def _insert_into_failed_database(self, failed_data_list):
        '''Insert cleaned data into failed database.

        Args:
            failed_data_list: List of friends data that can't be parsed.

        Raises:
            All exceptions raised by MongoClient,
            except for DuplicateKeyError.
        '''
        if failed_data_list:
            logger.info('Started inserting failed data into failed database. %s',
                        json.dumps({'collection': self._failed_collection.full_name}))
            try:
                insert_result = self._failed_collection.insert_many(failed_data_list)
            except BulkWriteError as e:
                # Ignore DuplicateKeyError
                n_error = len(e.details['writeErrors'])
                panic_list = [e_ for e_ in e.details['writeErrors']
                              if e_['code'] != DUPLICATE_KEY_ERROR_CODE]
                e.details['writeErrors'] = panic_list
                logger.info('Ignored duplications. %s',
                            json.dumps({'n_duplicate': n_error - len(panic_list)}))
                logger.info('Finished inserting failed data into failed database. %s',
                            json.dumps({'collection': self._failed_collection.full_name,
                                        'n_inserted': e.details['nInserted']}))
                if panic_list:
                    logger.exception('Exception occurred during insertion.',
                                     exc_info=e, stack_info=True)
                    raise e
            else:
                logger.info('Finished inserting failed data into failed database. %s',
                            json.dumps({'collection': self._failed_collection.full_name,
                                        'n_inserted': len(insert_result.inserted_ids)}))
            self._failed_collection.create_index(
                [('friend_code', ASCENDING), ('post_date', ASCENDING)],
                unique=True
            )

    def _connect_to_page(self):
        '''Webdriver connects to page.'''
        self._driver.get(self._URL)
        logger.info('Connected to url. %s',
                    json.dumps({'url': self._URL}, ensure_ascii=False))

    def _find_page_friends_section(self):
        '''Returns the friends section web element on page.

        Finds friends section on page and execute shadowRoot so that inner
        elements can be accessed.

        Requires _driver already connected to page.
        '''
        logger.info('Started finding friends section on page.')
        friends_section = self._driver.find_element_by_tag_name('gds-uma-musume-friends')
        friends_section = self._driver.execute_script('return arguments[0].shadowRoot', friends_section)
        # Ensure shadowRoot executed by finding elements inside it
        timeout_counter = 0
        while timeout_counter < self._TIMEOUT:
            if not friends_section.find_elements_by_class_name('-r-uma-musume-friends__search-wrap'):
                timeout_counter += 1
                time.sleep(1)
            else:
                break
        if timeout_counter == self._TIMEOUT:
            raise PageError('Cannot execute page shadowroot.')
        self._friends_section = friends_section
        logger.info('Finished finding friends section on page.')

    def _get_friend_element_list(self):
        '''Returns list of friend web element.

        Requires _friends_section being set.

        Difference this method and _parse_friend_html_list:
        This methods uses selenium, and returns web element list,
        while the other takes text as input, parses with BeautifulSoup,
        and returns html list.

        Returns:
            List of web elements.
        '''
        logger.info('Started getting friend html list.')
        friend_element_list = []
        for _ in range(self._TIMEOUT):
            if friend_element_list:
                break
            friend_element_list = self._friends_section.find_elements_by_class_name('-r-uma-musume-friends-list-item')
            time.sleep(1)

        if not friend_element_list:
            logger.error('Failed to get friend html list.')
            # TODO: raise exception
        logger.info('Finished getting friend html list. %s',
                    json.dumps({'count': len(friend_element_list)}))
        return friend_element_list

    def _is_friend_in_db(self, friend_element):
        '''Returns whether this friend is already in the database.

        Args:
            friend_element: A web element of a friend.
        '''
        raw_friend_html = friend_element.get_attribute('innerHTML')
        friend_html = BeautifulSoup(raw_friend_html, 'lxml')
        friend_data = self._get_friend_data(friend_html)

        friend_code = friend_data['friend_code']
        post_date = friend_data['post_date']
        query = {'friend_code': friend_code, 'post_date': post_date}
        matched_document = self._raw_collection.find_one(query)

        if matched_document is None:
            return False
        logger.info('Found duplicate friend data. %s',
                    json.dumps({'friend_code': friend_code, 'post_date': str(post_date)}))
        return True

    def _click_more_friends_button(self):
        '''Attempts to click the "もっと見る" button once.

        Returns:
            Bool indicating whether clicking is successful.
        '''
        timeout_counter = 0
        while timeout_counter < self._TIMEOUT:
            try:
                more_friends_button = self._friends_section.find_element_by_class_name('-r-uma-musume-friends__next')
            except NoSuchElementException:
                timeout_counter += 1
                time.sleep(1)
            else:
                self._driver.execute_script("arguments[0].click();", more_friends_button)
                return True
        return False

    def _load_more_friends(self):
        '''Repeatedly clicks the "もっと見る" button on page to load more friends.

        Stops clicking if one of the following conditions met:
            (1) Button click limit reached. Page crashes if it's too huge.
            (2) The database isn't empty and the oldest record on the
                current page is already in the database.
            (3) There's no button to be clicked.

        Requires _friends_section being set.
        '''
        logger.info('Started loading more friends on page.')
        button_click_count = 0
        logger.info('Started clicking button. %s',
                    json.dumps({'button': 'もっと見る', 'limit': self._BUTTON_LIMIT},
                               ensure_ascii=False))
        while True:
            # Condition (1)
            if button_click_count == self._BUTTON_LIMIT:
                logger.info('Reached button click limit. %s',
                            json.dumps({'limit': self._BUTTON_LIMIT}))
                break

            # Condition (2)
            if self._raw_collection.count_documents({}) != 0:
                friend_element_list = self._get_friend_element_list()
                last_friend_element = friend_element_list[-1]
                if self._is_friend_in_db(last_friend_element):
                    break

            # Condition (3)
            is_button_clicked = self._click_more_friends_button()
            if is_button_clicked:
                button_click_count += 1
                logger.info('Clicked button. %s',
                            json.dumps({'button': 'もっとみる', 'count': button_click_count},
                                       ensure_ascii=False))
                # Avoid rapid clicking
                time.sleep(2)
            else:
                logger.info('Button not found %s',
                            json.dumps({'button': 'もっと見る'}, ensure_ascii=False))
                break

        logger.info('Finished clicking button. %s',
                    json.dumps({'button': 'もっと見る', 'count': button_click_count},
                               ensure_ascii=False))
        logger.info('Finished loading more friends on page.')

    def _wait_load_friends_section(self):
        '''Waits for friends section to load.

        Logs how many records are searched and how many of them are shown on page.
        By default the 2 figures should match, because we are searching for ALL records,
        rather than querying under some conditions.
        If they don't match, it might be that the "もっと見る" button was clicked too
        frequently, new records were being searched but weren't loaded to the page.

        Requires _friends_section being set.
        '''
        logger.info('')
        timeout_counter = 0
        while timeout_counter < self._TIMEOUT:
            result = self._friends_section.find_elements_by_class_name('-r-uma-musume-friends__results')
            if not result:
                timeout_counter += 1
                time.sleep(1)
            else:
                break
        if timeout_counter == self._TIMEOUT:
            raise PageError('Cannot load friends section.')

        result = result[0].text
        log_data = {'result': result}
        try:
            # Example result: "直近800件について検索した結果は800件でした"
            # Match the numbers
            n_searched, n_loaded = re.findall(r'\d+', result)
        except ValueError as e:
            # Not enough / too many values to unpack
            logger.exception('Cannot fetch page loading result numbers.',
                             exc_info=e, stack_info=True)
        else:
            log_data['n_searched'] = n_searched
            log_data['n_loaded'] = n_loaded

        logger.info('Finished loading friends section. %s',
                    json.dumps(log_data, ensure_ascii=False))
        if n_searched != n_loaded:
            logger.warning('Number of searched results does not match number of loaded results. %s',
                           json.dumps(log_data, ensure_ascii=False))

    def _scrape_raw(self):
        '''Connects to url, scrapes the page, and finds friends section.

        Returns:
            HTML of the friends section web element.
        '''
        logger.info('Started scraping friends section.')
        raw_friends_html = None
        try:
            self._connect_to_page()
            self._find_page_friends_section()
            self._load_more_friends()
            self._wait_load_friends_section()
            raw_friends_html = self._friends_section.get_attribute('innerHTML')
        except Exception as e:
            logger.exception('An exception occurred during scraping.',
                             exc_info=e, stack_info=True)
            # Avoid premature exit leaving zombie process behind
            self._driver.quit()
            logger.info('Quitted webdriver.')
            # TODO: actually deal with all sorts of exceptions
            raise e
        else:
            self._driver.quit()
            logger.info('Quitted webdriver.')

        if raw_friends_html is None:
            logger.error('Failed to scrape friends section.')
            # TODO: raise exception
        logger.info('Finished scraping friends section.')
        return raw_friends_html
