import time
from pymongo import MongoClient
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from . import dbnames


url = "https://gamewith.jp/uma-musume/article/show/260740"
BUTTON_LIMIT = 200


def get_friends_list(friends_section):
    friends = []
    for _ in range(15):
        if friends:
            break
        print('Getting friends list...')
        friends = friends_section.find_elements_by_class_name('-r-uma-musume-friends-list-item')
        time.sleep(2)
    return friends


def scrape(tmp='scraped'):
    with MongoClient() as mongo_client:
        db = mongo_client[dbnames.raw_gamewith_uma_friends_db]
        raw_friends = db[dbnames.raw_friends_collection]

        try:
            chrome_op = webdriver.ChromeOptions()
            chrome_op.add_argument('headless')
            driver = webdriver.Chrome(options=chrome_op)
            print(f'Connecting to {url} ...')
            driver.get(url)

            # Get to the friends section
            friends_section = driver.find_element_by_tag_name('gds-uma-musume-friends')
            friends_section = driver.execute_script('return arguments[0].shadowRoot', friends_section)
            print('Waiting for page load...')
            # Ensure shadowRoot executed by finding elements inside it
            while True:
                if not friends_section.find_elements_by_class_name('-r-uma-musume-friends__search-wrap'):
                    time.sleep(0.5)
                else:
                    break

            # Repeatly click "もっと見る" button until conditions met.
            # Each time the page loads 200 more records.
            button_click_count = 0
            while True:
                # 1) Limit reached
                # Page crashes if it's too huge (~40000 records i.e. 200 clicks)
                if button_click_count == BUTTON_LIMIT:
                    break

                # If local MongoDB isn't empty
                if raw_friends.count_documents({}) != 0:
                    friends = get_friends_list(friends_section)
                    print(f'Fetched {len(friends)} friends...')
                    if not friends:
                        break

                    # Check if the last record on page is already in db.
                    last_friend = friends[-1]
                    trainer_id = (last_friend
                                  .find_element_by_class_name('-r-uma-musume-friends-list-item__trainerId__text')
                                  .text)
                    post_date = (last_friend
                                 .find_element_by_class_name('-r-uma-musume-friends-list-item__postDate')
                                 .text)
                    # 2) Record already in db
                    if raw_friends.count_documents({'friend_code': trainer_id, 'post_date': post_date}) != 0:
                        print(f'Record already in database: ({trainer_id}, {post_date})')
                        print('Stop clicking.')
                        break

                # Try to click "もっと見る" button
                button_clicked = False
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
                        # ElementClickInterceptedException: is not clickable at point (450, 515). Other element would receive the click
                        driver.execute_script("arguments[0].click();", more_friends_button)
                        print('もっと見る button clicked.')
                        button_clicked = True
                        button_click_count += 1
                        # Avoid rapid clicking
                        time.sleep(2)
                        break

                # 3) Can't find "もっと見る" button
                if not button_clicked:
                    break

            print('Waiting for page load...')
            time.sleep(10)
            # Says how many records searched and how many of them are shown on page.
            # By default the 2 figures should match, because we are searching for ALL records,
            # rather than querying under some conditions.
            # If the "もっと見る" button is pressed too fast, new records are searched but won't
            # be loaded to the page.
            print(friends_section.find_element_by_class_name('-r-uma-musume-friends__results').text)

            print(f'Writing to file: {tmp}')
            with open(tmp, 'w', encoding='utf8') as f:
                f.write(friends_section.get_attribute('innerHTML'))

        except Exception as e:
            # To avoid premature exits leaving zombie processes
            driver.quit()
            raise e

        driver.quit()


if __name__ == '__main__':
    scrape()
