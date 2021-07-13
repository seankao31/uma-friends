# Uma Friends

# Notes
## For selenium in heroku
```
heroku buildpacks:add https://github.com/heroku/heroku-buildpack-google-chrome
heroku buildpacks:add https://github.com/heroku/heroku-buildpack-chromedriver
```

## For local testing (not using heroku local)
at local database
```
source .env.test && export $(cut -d= -f1 .env.test)
python scrape_friends_gamewith.py
```
at atlas database
```
source .env && export $(cut -d= -f1 .env)
python scrape_friends_gamewith.py
```
