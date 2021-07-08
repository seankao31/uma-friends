import time

from bs4 import BeautifulSoup
from progress.bar import IncrementalBar
from pymongo import DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException


# game data
UMA_MUSUME_GAME_DB = 'uma_musume_game'

# friend raw data scraped from gamewith
RAW_GAMEWITH_UMA_FRIENDS_DB = 'raw_gamewith_uma_friends'
RAW_FRIENDS_COLLECTION = 'raw_friends'

# cleaned up friend data
UMA_FRIENDS_DB = 'uma_friends'
UMA_FRIENDS_COLLECTION = 'friends'


GAMEWITH_FRIENDS_URL = "https://gamewith.jp/uma-musume/article/show/260740"
BUTTON_LIMIT = 200


def get_friends_list(friends_section):
    '''Returns the list of friends presented in friends_section.'''

    friends = []
    for _ in range(15):
        if friends:
            break
        print('Getting friends list...')
        friends = friends_section.find_elements_by_class_name('-r-uma-musume-friends-list-item')
        time.sleep(2)
    return friends


def get_friends_section(driver):
    '''Finds friends_section in page and execute shadowRoot. Returns friends_section.'''

    friends_section = driver.find_element_by_tag_name('gds-uma-musume-friends')
    friends_section = driver.execute_script('return arguments[0].shadowRoot', friends_section)
    print('Waiting for page load...')
    # Ensure shadowRoot executed by finding elements inside it
    while True:
        if not friends_section.find_elements_by_class_name('-r-uma-musume-friends__search-wrap'):
            time.sleep(0.5)
        else:
            break
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
        print(f'Record already in database: ({trainer_id}, {post_date})')
        return True

    return False


def click_more_friends_button(driver, friends_section):
    '''Attempts to click "もっと見る" button. Returns whether it's successful.'''

    try_more_button_cnt = 0
    while try_more_button_cnt <= 10:
        try:
            print('Finding もっと見る button...')
            more_friends_button = friends_section.find_element_by_class_name('-r-uma-musume-friends__next')
        except NoSuchElementException:
            try_more_button_cnt += 1
            time.sleep(1)
        else:
            # more_friends_button.click()  # won't work
            # above will throw ElementClickInterceptedException
            driver.execute_script("arguments[0].click();", more_friends_button)
            print('もっと見る button clicked.')
            return True
    return False


def scrape_raw(url):
    '''Scrapes url and returns raw friend list html.'''

    with MongoClient() as mongo_client:
        db = mongo_client[RAW_GAMEWITH_UMA_FRIENDS_DB]
        raw_friends = db[RAW_FRIENDS_COLLECTION]

        try:
            chrome_op = webdriver.ChromeOptions()
            chrome_op.add_argument('headless')
            driver = webdriver.Chrome(options=chrome_op)
            print(f'Connecting to {url} ...')
            driver.get(url)

            # Get the friends section on the page
            friends_section = get_friends_section(driver)

            # Repeatly click "もっと見る" button until conditions met.
            # Each time the page loads 200 more records.
            button_click_count = 0
            while True:
                # 1) Limit reached
                # Page crashes if it's too huge (~40000 records i.e. 200 clicks)
                if button_click_count == BUTTON_LIMIT:
                    break

                # If db isn't empty
                if raw_friends.count_documents({}) != 0:
                    friends = get_friends_list(friends_section)
                    print(f'Fetched {len(friends)} friends...')
                    if not friends:
                        break

                    # 2) Last (oldest) record on current page is already in db
                    last_friend = friends[-1]
                    if is_friend_in_db(friend=last_friend, collection=raw_friends):
                        print('Stop clicking.')
                        break

                # Try to click "もっと見る" button
                is_button_clicked = click_more_friends_button(driver, friends_section)
                if is_button_clicked:
                    button_click_count += 1
                    # Avoid rapid clicking
                    time.sleep(2)
                else:
                    # 3) Can't find "もっと見る" button
                    break

            print('Waiting for page load...')
            time.sleep(10)

            # Prints out how many records are searched and how many of them are shown on page.
            # By default the 2 figures should match, because we are searching for ALL records,
            # rather than querying under some conditions.
            # If the "もっと見る" button is pressed too fast, new records are searched but won't
            # be loaded to the page.
            print(friends_section.find_element_by_class_name('-r-uma-musume-friends__results').text)

            page_content = friends_section.get_attribute('innerHTML')

        except Exception as e:
            # To avoid premature exits leaving zombie processes
            driver.quit()
            # We don't really deal with the exception though, hence raising it
            raise e

        driver.quit()

    return page_content


def parse_page(page_html):
    '''Parses page_html. Returns list of ul elements each containing data of one friend.'''

    print('Parsing page html...')
    page = BeautifulSoup(page_html, 'lxml')

    friends = list(page.ul)
    # Reverse to insert in the order they're uploaded to the website
    friends.reverse()
    return friends


def get_friends(friends_page_list):
    '''Generator that yields entries. Each entry is a dict that contains friend information we need.'''

    progress_bar = IncrementalBar('Processing', max=len(friends_page_list))
    for friend in progress_bar.iter(friends_page_list):
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

        yield entry


if __name__ == '__main__':
    raw_page = scrape_raw(url=GAMEWITH_FRIENDS_URL)
    parsed_list = parse_page(page_html=raw_page)
    n_friends = len(parsed_list)
    friends = get_friends(friends_page_list=parsed_list)

    mongo_client = MongoClient()
    raw_db = mongo_client[RAW_GAMEWITH_UMA_FRIENDS_DB]
    raw_friends = raw_db[RAW_FRIENDS_COLLECTION]
    raw_friends.create_index(
        [('friend_code', DESCENDING), ('post_date', DESCENDING)],
        unique=True
    )

    print(f'Inserting into database {RAW_GAMEWITH_UMA_FRIENDS_DB}/{RAW_FRIENDS_COLLECTION}')
    duplicate_count = 0
    for friend in friends:
        try:
            raw_friends.insert_one(friend)
        except DuplicateKeyError:
            duplicate_count += 1
    num_new_records = n_friends - duplicate_count
    print(f'New records: {num_new_records}')