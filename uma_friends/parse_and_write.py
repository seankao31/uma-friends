from bs4 import BeautifulSoup
from pymongo import MongoClient, DESCENDING
from pymongo.errors import DuplicateKeyError
from . import dbnames


# Print iterations progress
def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd='\r'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def parse_and_write(tmp='scraped'):
    with MongoClient() as mongo_client:
        db = mongo_client[dbnames.raw_gamewith_uma_friends_db]
        raw_friends = db[dbnames.raw_friends_collection]
        raw_friends.create_index(
            [('friend_code', DESCENDING), ('post_date', DESCENDING)],
            unique=True
        )

        with open(tmp, 'r', encoding='utf8') as f:
            print(f'Reading file: {tmp}')
            page_html = f.read()
            print('Parsing file...')
            page = BeautifulSoup(page_html, features="html.parser")

            friends = list(page.ul)
            # Insert in the order they're uploaded to the website
            friends.reverse()
            n = len(friends)

            print(f'Inserting into database {dbnames.raw_gamewith_uma_friends_db}/{dbnames.raw_friends_collection}')
            if n != 0:
                print_progress_bar(0, n, prefix='Progress:', suffix='Complete', length=50)
            duplicate_count = 0
            for i, friend in enumerate(friends):
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
                print_progress_bar(i+1, n, prefix='Progress:', suffix='Complete', length=50)
                try:
                    raw_friends.insert_one(entry)
                except DuplicateKeyError:
                    duplicate_count += 1
                    continue
            print('Finished!')
            num_new_records = n - duplicate_count
            print(f'New records: {num_new_records}')


if __name__ == '__main__':
    parse_and_write()
