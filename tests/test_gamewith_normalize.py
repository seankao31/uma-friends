from pymongo import MongoClient
import pytest

from uma_friends.gamewith_normalizer import GamewithNormalizer


@pytest.fixture
def database():
    UMAFRIENDS_DB_URI = "localhost:27017"
    GAME_DATA_DB = "test_uma_musume_game"
    mongo_client = MongoClient(UMAFRIENDS_DB_URI)
    return mongo_client[GAME_DATA_DB]


@pytest.fixture
def normalizer(database):
    return GamewithNormalizer(database)


def test_find_skill_id_and_uniqueness_by_name_common(normalizer):
    skill_name = 'ギアシフト'
    skill_id = 'kjP0LurWRte'
    is_unique = False

    result_id, result_is_unique = normalizer._find_skill_id_and_uniqueness_by_name(skill_name)

    assert result_id == skill_id
    assert result_is_unique == is_unique
    assert skill_name in normalizer._cache['find_skill_by_name']
    assert normalizer._cache['find_skill_by_name'][skill_name] == (skill_id, is_unique)


def test_find_skill_id_and_uniqueness_by_name_unique(normalizer):
    skill_name = '波乱注意砲！'
    skill_id = 'W2FgL9QXA_9'
    is_unique = True

    result_id, result_is_unique = normalizer._find_skill_id_and_uniqueness_by_name(skill_name)

    assert result_id == skill_id
    assert result_is_unique == is_unique
    assert skill_name in normalizer._cache['find_skill_by_name']
    assert normalizer._cache['find_skill_by_name'][skill_name] == (skill_id, is_unique)


def test_find_skill_id_and_uniqueness_by_name_not_exist(normalizer):
    skill_name = 'FAKE SKILL'

    result_id, result_is_unique = normalizer._find_skill_id_and_uniqueness_by_name(skill_name)

    # (None, False)
    assert result_id is None
    assert not result_is_unique
    assert skill_name in normalizer._cache['find_skill_by_name']
    assert normalizer._cache['find_skill_by_name'][skill_name] == (None, False)


def test_find_uma_id_by_unique_skill(normalizer):
    unique_skill_id = 'aIKAfks7LQR'
    uma_id = 'BJTPrfuBw_U'

    result = normalizer._find_uma_id_by_unique_skill(unique_skill_id)

    assert result == uma_id
    assert unique_skill_id in normalizer._cache['find_uma_by_unique_skill']
    assert normalizer._cache['find_uma_by_unique_skill'][unique_skill_id] == uma_id


def test_find_uma_id_by_unique_skill_not_exist(normalizer):
    unique_skill_id = 'FAKESKILLID'

    result = normalizer._find_uma_id_by_unique_skill(unique_skill_id)

    assert result is None
    assert unique_skill_id in normalizer._cache['find_uma_by_unique_skill']
    assert normalizer._cache['find_uma_by_unique_skill'][unique_skill_id] is None


def test_find_race_id_by_name(normalizer):
    race_name = 'サウジアラビアロイヤルカップ'
    race_id = 'AbhqdP7Nkof'

    result = normalizer._find_race_id_by_name(race_name)

    assert result == race_id
    assert race_name in normalizer._cache['find_race_by_name']
    assert normalizer._cache['find_race_by_name'][race_name] == race_id


def test_find_race_id_by_name_not_exist(normalizer):
    race_name = 'FAKE RACE'

    result = normalizer._find_race_id_by_name(race_name)

    assert result is None
    assert race_name in normalizer._cache['find_race_by_name']
    assert normalizer._cache['find_race_by_name'][race_name] is None


def test_parse_factors(normalizer):
    factor_string_list = [
        'パワー3(代表3)',
        'スタミナ6',
        '差し2(代表2)',
        'マイル4',
        'Pride of KING1(代表1)',
        '紅焔ギア/LP1211-M1',
        'Shadow Break1',
        '集中力1(代表1)',
        '末脚3',
        '日本ダービー1',
        'URAシナリオ6(代表3)',
        '有馬記念1'
    ]
    factors = [
        {
            'name': 'パワー',
            'type': 'blue',
            'total_level': 3,
            'main_level': 3
        },
        {
            'name': 'スタミナ',
            'type': 'blue',
            'total_level': 6
        },
        {
            'name': '差し',
            'type': 'strategy',
            'total_level': 2,
            'main_level': 2
        },
        {
            'name': 'マイル',
            'type': 'distance',
            'total_level': 4
        },
        {
            'name': 'Pride of KING',
            'type': 'unique_skill',
            'total_level': 1,
            'main_level': 1
        },
        {
            'name': '紅焔ギア/LP1211-M',
            'type': 'unique_skill',
            'total_level': 1
        },
        {
            'name': 'Shadow Break',
            'type': 'unique_skill',
            'total_level': 1
        },
        {
            'name': '集中力',
            'type': 'common_skill',
            'total_level': 1,
            'main_level': 1
        },
        {
            'name': '末脚',
            'type': 'common_skill',
            'total_level': 3
        },
        {
            'name': '日本ダービー',
            'type': 'race',
            'total_level': 1
        },
        {
            'name': 'URAシナリオ',
            'type': 'ura',
            'total_level': 6,
            'main_level': 3
        },
        {
            'name': '有馬記念',
            'type': 'race',
            'total_level': 1
        }
    ]

    result_factors = normalizer._parse_factors(factor_string_list)

    assert len(factors) == 12
    assert result_factors == factors
