from datetime import datetime, timezone
import hashlib
import json
import logging


def get_utc_datetime(date_string, format):
    '''Returns datetime (in utc timezone) based on date_string and format.

    This function assumes that the given date_string does not
    contain year info. It assigns a year so that the resulting datetime
    is the latest datetime that's earlier than NOW.

    Args:
        The same as datetime.strptime() function. Example usage:
        get_utc_datetime('07/16 13:22', '%m/%d %H:%M')
    '''
    now_date_local = datetime.now()
    now_year_local = now_date_local.year

    post_date_local = datetime.strptime(date_string, format)
    # Add year info
    post_date_local = post_date_local.replace(year=now_year_local)
    if post_date_local > now_date_local:
        # Year has changed since post date
        post_date_local = post_date_local.replace(year=now_year_local - 1)

    post_date_utc = post_date_local.astimezone(tz=timezone.utc)
    return post_date_utc


def hash_object(obj):
    '''Returns sha1 hash of object. Attempts formalizing object before hashing.

    Args:
        obj: A dict to be hashed. All values should be serializable.
    '''
    object_str = json.dumps(obj,
                            sort_keys=True,
                            separators=(',', ':'),
                            ensure_ascii=True,
                            indent=None)
    return hashlib.sha1(object_str.encode('utf-8')).hexdigest()


def get_logger():
    logger = logging.getLogger('uma_friends')
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
