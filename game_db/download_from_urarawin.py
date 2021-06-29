import json
import os
import requests


urarawin_db_url = 'https://raw.githubusercontent.com/wrrwrr111/pretty-derby/master/src/assert/db.json'
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

print()
print('====== Start of commands ======')
print()

for k, v in db.items():
    if not isinstance(v, list) and not isinstance(v, dict):
        with open(k, 'w', encoding='utf8') as f:
            json.dump(v, f, ensure_ascii=False)
        continue

    file_name = f'{k}.json'
    with open(f'{dir}/{file_name}', 'w', encoding='utf8') as ff:
        json.dump(v, ff, ensure_ascii=False)
        command = f'mongoimport --db uma_musume_game --drop --file {dir}/{file_name}'
        if isinstance(v, list):
            command += ' --jsonArray'
        print(command)

# somehow subprocess gives error when mongoimport
# so the workaround is let the user execute these commands
print()
print('====== End of commands ======')
print('Copy and paste above commands in shell')
