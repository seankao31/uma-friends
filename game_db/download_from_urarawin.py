import json
import os
import shutil
import subprocess

import requests
from bs4 import BeautifulSoup
from progress.bar import IncrementalBar
from pymongo import MongoClient


uma_musume_game_db = 'uma_musume_game'
urarawin_db_url = 'https://raw.githubusercontent.com/wrrwrr111/pretty-derby/master/src/assert/db.json'
uma_article_base_url = 'https://gamewith.jp/uma-musume/article/show/'


def download():
    print('Downloading...')
    db = requests.get(urarawin_db_url).json()
    file_name = 'urarawin_db.json'  # for inspection
    print(f'Writing to {file_name}')
    with open(file_name, 'w', encoding='utf8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

    dir = 'collections'
    try:
        os.mkdir(dir)
    except FileExistsError:
        pass

    uma_musume_game_db = 'uma_musume_game'

    for k, v in db.items():
        if not isinstance(v, list) and not isinstance(v, dict):
            with open(k, 'w', encoding='utf8') as f:
                json.dump(v, f, ensure_ascii=False)
            continue

        file_name = f'{k}.json'
        with open(f'{dir}/{file_name}', 'w', encoding='utf8') as ff:
            json.dump(v, ff, ensure_ascii=False)
            command = f'mongoimport --db {uma_musume_game_db} --drop --file {dir}/{file_name}'
            if isinstance(v, list):
                command += ' --jsonArray'
            print(command)
        subprocess.run(command, shell=True)

    shutil.rmtree(dir)


def map_uma_to_img_url():
    with MongoClient() as mongo_client:
        db = mongo_client[uma_musume_game_db]
        collection = db['players']

        print()
        print('Mapping uma to gamewith img url...')

        umas = collection.find()
        progress_bar = IncrementalBar('Processing', max=collection.count_documents({}))
        for uma in progress_bar.iter(umas):
            gwId = uma['gwId']
            url = uma_article_base_url + gwId
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'lxml')
            a = soup.find('a', href=url)
            gwImgUrl = a.img['data-original']
            collection.update_one({'_id': uma['_id']}, {'$set': {'gwImgUrl': gwImgUrl}})


if __name__ == '__main__':
    download()
    map_uma_to_img_url()
