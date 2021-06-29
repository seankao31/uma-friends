from uma_friends.scrape_raw import scrape
from uma_friends.parse_and_write import parse_and_write
import os


TMP = 'tmp_scraped'

if __name__ == '__main__':
    scrape(tmp=TMP)
    parse_and_write(tmp=TMP)
    os.remove(TMP)
    print(f'File {TMP} removed.')
