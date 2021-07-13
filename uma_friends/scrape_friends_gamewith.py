import configparser
import json
import logging
import os
import time

from bs4 import BeautifulSoup
from pymongo import DESCENDING, MongoClient
from pymongo.errors import BulkWriteError
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException


env = 'PROD'
# PROD or TEST
config = configparser.ConfigParser()
config.read(os.path.abspath(os.path.join(".ini")))


def get_logger():
    logger = logging.getLogger('scrape_friends_gamewith')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


logger = get_logger()

# Sadly it seems that there's no other way other than using magic number
DUPLICATE_KEY_ERROR_CODE = 11000
BUTTON_LIMIT = int(config[env]['BUTTON_LIMIT'])

# game data
UMA_MUSUME_GAME_DB = 'uma_musume_game'

# friend raw data scraped from gamewith
UMAFRIENDS_DB_URI = config[env]['UMAFRIENDS_DB_URI']
UMAFRIENDS_DB = config[env]['UMAFRIENDS_DB']
RAW_GAMEWITH_FRIENDS_NS = config[env]['RAW_GAMEWITH_FRIENDS_NS']

# cleaned up friend data
UMA_FRIENDS_DB = 'uma_friends'
UMA_FRIENDS_COLLECTION = 'friends'


GAMEWITH_FRIENDS_URL = config['URL']['GAMEWITH_FRIENDS_URL']


class PageError(Exception):
    pass


def message_with_json(msg, json_object):
    return msg + ' ' + json.dumps(json_object, ensure_ascii=False)


def get_friends_list(friends_section):
    '''Returns the list of friends presented in friends_section.'''

    friends = []
    for _ in range(15):
        if friends:
            break
        friends = friends_section.find_elements_by_class_name('-r-uma-musume-friends-list-item')
        time.sleep(2)
    logger.info('Got friend list on page.')
    return friends


def get_friends_section(driver, timeout=30):
    '''Finds friends_section in page and execute shadowRoot. Returns friends_section.'''

    friends_section = driver.find_element_by_tag_name('gds-uma-musume-friends')
    friends_section = driver.execute_script('return arguments[0].shadowRoot', friends_section)
    # Ensure shadowRoot executed by finding elements inside it
    timeout_counter = 0
    while timeout_counter < timeout:
        if not friends_section.find_elements_by_class_name('-r-uma-musume-friends__search-wrap'):
            timeout_counter += 1
            time.sleep(1)
        else:
            break
    if timeout_counter == timeout:
        raise PageError('Cannot execute page shadowroot.')
    logger.info('Loaded page shadowroot.')
    return friends_section


def is_friend_in_db(friend, collection):
    '''Returns whether friend is already in collection.'''

    trainer_id = (friend
                  .find_element_by_class_name('-r-uma-musume-friends-list-item__trainerId__text')
                  .text)
    post_date = (friend
                 .find_element_by_class_name('-r-uma-musume-friends-list-item__postDate')
                 .text)
    if collection.count_documents({'friend_code': trainer_id, 'post_date': post_date}) != 0:
        message = message_with_json('Found duplicate record on page.',
                                    {'friend_code': trainer_id, 'post_date': post_date})
        logger.info(message)
        return True

    return False


def click_more_friends_button(driver, friends_section, timeout=20):
    '''Attempts to click "もっと見る" button. Returns whether it's successful.'''

    timeout_counter = 0
    while timeout_counter <= timeout:
        try:
            more_friends_button = friends_section.find_element_by_class_name('-r-uma-musume-friends__next')
        except NoSuchElementException:
            timeout_counter += 1
            time.sleep(1)
        else:
            # more_friends_button.click()  # won't work
            # above will throw ElementClickInterceptedException
            driver.execute_script("arguments[0].click();", more_friends_button)
            return True
    return False


def scrape_raw(url, timeout=100):
    '''Scrapes url and returns raw friend list html.'''

    with MongoClient(UMAFRIENDS_DB_URI) as mongo_client:
        db = mongo_client[UMAFRIENDS_DB]
        raw_friends = db[RAW_GAMEWITH_FRIENDS_NS]

        try:
            chrome_op = webdriver.ChromeOptions()
            chrome_op.add_argument('headless')
            driver = webdriver.Chrome(options=chrome_op)
            driver.get(url)
            logger.info(message_with_json('Connected to page.', {'url': url}))

            # Get the friends section on the page
            friends_section = get_friends_section(driver)

            # Repeatly click "もっと見る" button until conditions met.
            # Each time the page loads 200 more records.
            button_click_count = 0
            logger.info(message_with_json('Start clicking button.', {'limit': BUTTON_LIMIT}))
            while True:
                # 1) Limit reached
                # Page crashes if it's too huge (~40000 records i.e. 200 clicks)
                if button_click_count == BUTTON_LIMIT:
                    logger.info(message_with_json('Reached button click limit.', {'limit': BUTTON_LIMIT}))
                    break

                # If db isn't empty
                if raw_friends.count_documents({}) != 0:
                    friends = get_friends_list(friends_section)
                    logger.info(message_with_json('Fetched friends.', {'count': len(friends)}))
                    if not friends:
                        logger.warning(message_with_json('There are no friends on page.', {'url': url}))
                        break

                    # 2) Last (oldest) record on current page is already in db
                    last_friend = friends[-1]
                    if is_friend_in_db(friend=last_friend, collection=raw_friends):
                        break

                # Try to click "もっと見る" button
                is_button_clicked = click_more_friends_button(driver, friends_section)
                if is_button_clicked:
                    button_click_count += 1
                    logger.info(message_with_json('Clicked button.', {'button': 'もっと見る', 'count': button_click_count}))
                    # Avoid rapid clicking
                    time.sleep(2)
                else:
                    # 3) Can't find "もっと見る" button
                    logger.info(message_with_json('Button not found.', {'button': 'もっと見る'}))
                    break

            logger.info(message_with_json('Stopped clicking button.', {'button': 'もっと見る'}))

            timeout_counter = 0
            while timeout_counter < timeout:
                page_result = friends_section.find_elements_by_class_name('-r-uma-musume-friends__results')
                if not page_result:
                    timeout_counter += 1
                    time.sleep(1)
                else:
                    # Logs how many records are searched and how many of them are shown on page.
                    # By default the 2 figures should match, because we are searching for ALL records,
                    # rather than querying under some conditions.
                    # If the "もっと見る" button is pressed too frequently, new records are searched but won't
                    # be loaded to the page.
                    logger.info(message_with_json('Friend section loaded.', {'result': page_result[0].text}))
                    break
            if timeout_counter == timeout:
                raise PageError('Cannot load friend section.')

            page_content = friends_section.get_attribute('innerHTML')

        except Exception as e:
            # To avoid premature exits leaving zombie processes
            driver.quit()
            logger.exception()
            # We don't really deal with the exception though, hence raising it
            raise e

        driver.quit()

    return page_content


def parse_page(page_html):
    '''Parses page_html. Returns list of ul elements each containing data of one friend.'''

    page = BeautifulSoup(page_html, 'lxml')
    logger.info('Parsed html.')

    friends = list(page.ul)
    return friends


def get_friends(friends_page_list):
    '''Return list of friends, each entry is a dict that contains friend information we need.'''

    friends = []
    for friend in friends_page_list:
        support_id = None
        support_wrap = friend.find_all(class_='-r-uma-musume-friends-list-item__support-wrap')
        if support_wrap:
            support_id = support_wrap[0].find_all('a')[0].get('href')
            support_id = support_id.split('/')[-1]

        support_limit = None
        support_limit_wrap = friend.find_all(class_='-r-uma-musume-friends-list-item__limitNumber')
        if support_limit_wrap:
            support_limit = support_limit_wrap[0].text.strip()

        trainer_id = None
        trainer_id_wrap = friend.find_all(class_='-r-uma-musume-friends-list-item__trainerId__text')
        if trainer_id_wrap:
            trainer_id = trainer_id_wrap[0].text.strip()

        main_uma_img = None
        main_uma_wrap = friend.find_all(class_='-r-uma-musume-friends-list-item__mainUmaMusume-wrap')
        if main_uma_wrap:
            main_uma_img = main_uma_wrap[0].img.get('src').strip()

        factors = None
        factors_item = friend.find_all(class_='-r-uma-musume-friends-list-item__factor-list__item')
        if factors_item:
            factors = [factor.text.strip() for factor in factors_item]

        comment = None
        comment_wrap = friend.find_all(class_='-r-uma-musume-friends-list-item__comment')
        if comment_wrap:
            comment = comment_wrap[0].text.strip()

        post_date = None
        post_date_wrap = friend.find_all(class_='-r-uma-musume-friends-list-item__postDate')
        if post_date_wrap:
            post_date = post_date_wrap[0].text.strip()

        entry = {
            'friend_code': trainer_id,
            'support_id': support_id,
            'support_limit': support_limit,
            'character_image_url': main_uma_img,
            'factors': factors,
            'comment': comment,
            'post_date': post_date
        }

        friends.append(entry)

    return friends


if __name__ == '__main__':
    raw_page = scrape_raw(url=GAMEWITH_FRIENDS_URL)
    parsed_list = parse_page(page_html=raw_page)
    friends = get_friends(friends_page_list=parsed_list)
    logger.info(message_with_json('Fetched friends on page to insert.', {'count': len(friends)}))

    mongo_client = MongoClient(UMAFRIENDS_DB_URI)
    raw_db = mongo_client[UMAFRIENDS_DB]
    raw_friends = raw_db[RAW_GAMEWITH_FRIENDS_NS]
    raw_friends.create_index(
        [('friend_code', DESCENDING), ('post_date', DESCENDING)],
        unique=True
    )

    message = message_with_json('Insert into database.',
                                {'uri': UMAFRIENDS_DB_URI, 'database': UMAFRIENDS_DB, 'collection': RAW_GAMEWITH_FRIENDS_NS})
    logger.info(message)
    try:
        insert_result = raw_friends.insert_many(friends, ordered=False)
    except BulkWriteError as e:
        n_error = len(e.details['writeErrors'])
        panic_list = [err for err in e.details['writeErrors'] if err['code'] != DUPLICATE_KEY_ERROR_CODE]
        n_duplicate_key_error = n_error - len(panic_list)
        logger.debug(message_with_json('Ignored duplicate key error.', {'count': n_duplicate_key_error}))
        e.details['writeErrors'] = panic_list
        logger.info(message_with_json('Insert finished.', {'nInserted': e.details["nInserted"]}))
        if panic_list:
            logger.exception()
            raise e
    else:
        logger.info(message_with_json('Insert finished.', {'nInserted': len(insert_result.inserted_ids)}))
