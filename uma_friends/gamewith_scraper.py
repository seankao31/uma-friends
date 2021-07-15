import logging

from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


class GamewithScraper:
    '''A web scraper that fetches friend data from gamewith website.

    Attributes:
        url:
            A string of url link to the gamewith uma musume
            friends sharing page.
        timeout:
            An integer indicating in seconds how long the scraper
            should keep trying to access some web elements.
        raw_collection:
            A pymongo Collection.
    '''
    def __init__(self, url, timeout, raw_collection):
        self.url = url
        self.timeout = timeout
        self.raw_collection = raw_collection

    def run(self):
        '''Starts scraping and storing data.'''
        raw_friends_html = self._scrape_raw()
        if raw_friends_html is None:
            logger.error()
            # raise exception: can't scrape friend section
            pass

        friend_html_list = self._parse_friend_html_list(raw_friends_html)
        if not friend_html_list:
            logger.error()
            # raise exception: can't fetch friend html list
            pass

        friends_data = self._get_friends_data(friend_html_list)
        self._insert_into_raw_database(friends_data)

    def _scrape_raw(self):
        '''Connects to url, scrapes the page, and finds friends section.

        Returns:
            None, if couldn't find friends section;
            HTML of the friends section web element, otherwise.
        '''
        raw_friends_html = None
        return raw_friends_html

    def _parse_friend_html_list(self, raw_friends_html):
        '''Parse friends section html and find list of friend html.

        Args:
            raw_friends_html:
                A string of the friends section html.

        Returns:
            List of <li> elements.
        '''
        soup = BeautifulSoup(raw_friends_html, 'lxml')
        friend_html_list = soup.find_all(class_='-r-uma-musume-friends-list-item')
        return friend_html_list

    def _get_friends_data(self, friend_html_list):
        '''Extract friends data from parsed friend html list.

        Args:
            friend_html_list:
                List of <li> elements.

        Returns:
            List of dicts consisting of friends data.
        '''
        friends_data = []
        for friend_html in friend_html_list:
            friend_data = self._get_friend_data(friend_html)
            friends_data.append(friend_data)
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
             'post_date': ISODate('2021-07-15T10:09:00Z'),
             'hash_digest': '5da3e135f5239bfd630fa36495ffb752161da5c2'}

             Values may be None if corresponding data isn't found.
        '''
        friend_data = {}
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
        pass
