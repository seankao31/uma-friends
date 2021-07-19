'''
Example friend data from gamewith:
{
    'friend_code': '248605600',
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
    'comment': 'スタミナ6\nパワー3\nマイル4\n差し2\nURA5\n\nキャンサー杯用に良かったら使って下さい。\n白因子省略\n代表URA☆3\n親2URA☆2',
    'post_date': ISODate('2021-07-15T10:09:00Z'),
    'hash_digest': '5da3e135f5239bfd630fa36495ffb752161da5c2'
}
'''
import copy
import json
import logging


logger = logging.getLogger(__name__)


class OutdatedError(Exception):
    pass


class GamewithNormalizer:
    _BLUES = [
        'スピード',
        'スタミナ',
        'パワー',
        '根性',
        '賢さ'
    ]
    _FIELD_TYPES = [
        '芝',
        'ダート'
    ]
    _DISTANCES = [
        '短距離',
        'マイル',
        '中距離',
        '長距離'
    ]
    _STRATEGIES = [
        '逃げ',
        '先行',
        '差し',
        '追込'
    ]

    def __init__(self, game_data_database):
        self._game_data_database = game_data_database
        self._cache = {
            'find_skill_by_name': {},
            'find_race_by_name': {},
            'find_uma_by_unique_skill': {}
        }

    def normalize(self, friend_data):
        friend = {}

        friend['friend_code'] = friend_data['friend_code']
        friend['comment'] = friend_data['comment']
        friend['post_date'] = friend_data['post_date']
        friend['main_uma'] = None
        friend['factors'] = None
        friend['parents'] = None

        support = {}
        if friend_data['support_id'] is not None:
            support['id'] = friend_data['support_id']
        if friend_data['support_limit'] is not None:
            # Take only the number part
            support['limit'] = friend_data['support_limit'][0]
        if not support:
            support = None
        friend['support'] = support

        if friend_data['character_image_url'] is None:
            return friend

        main_uma = {}
        try:
            main_uma_id = self._find_uma_id_by_image_url(friend_data['character_image_url'])
        except OutdatedError as e:
            logger.exception('Cannot find uma with image url. %s',
                             json.dumps({'image_url': friend_data['character_image_url']}),
                             esc_info=e,
                             stack_info=True)
            # TODO: insert into a FAILED_BUFFER database for future update
            return
        main_uma['id'] = main_uma_id

        if friend_data['factors'] is None:
            friend['main_uma'] = main_uma
            return friend

        factors = self._parse_factors(friend_data['factors'])
        main_uma_factors, total_factors = self._extract_main_and_total_factors(factors, main_uma_id)

        main_uma['factors'] = main_uma_factors
        friend['main_uma'] = main_uma
        friend['factors'] = total_factors

        unique_skill_factors = [factor for factor in total_factors
                                if factor['type'] == 'unique_skill']
        parents = self._guess_parents_by_unique_skill_factors(unique_skill_factors, main_uma_id)
        friend['parents'] = parents

        return friend

    def _find_uma_id_by_image_url(self, image_url):
        '''Returns uma id corresponding to image url.

        Args:
            image_url: A string of url.

        Raises:
            OutdatedError, if not found.
        '''
        uma = self._game_data_database['players'].find_one(
            {'gwImgUrl': image_url},
            {'_id': 0, 'id': 1}
        )
        if uma is None:
            raise OutdatedError('Cannot find uma in database.')
        return uma['id']

    def _extract_main_and_total_factors(self, factors, main_uma_id):
        '''Returns a list of main uma factors and a list of total factors.

        Args:
            factors:
                List of dict (parsed factors).
            main_uma_id:
                A string of the id of main uma.
        '''
        main_uma_factors = []
        for factor in factors:
            if 'main_level' in factor:
                factor_ = copy.deepcopy(factor)
                # Does not need total_level
                del factor_['total_level']
                # main_level as level instead
                factor_['level'] = factor_.pop('main_level')
                main_uma_factors.append(factor_)

        total_factors = []
        for factor in factors:
            factor_ = copy.deepcopy(factor)
            # Does not need main_level
            if 'main_level' in factor_:
                del factor_['main_level']
            # total_level as level instead
            factor_['level'] = factor_.pop('total_level')
            total_factors.append(factor_)

        return main_uma_factors, total_factors

    def _parse_factors(self, factor_string_list):
        '''Returns list of parsed factors.

        Args:
            factor_string_list: List of string.
        '''
        return [self._parse_factor(factor_string)
                for factor_string in factor_string_list]

    def _parse_factor(self, factor_string):
        '''Returns parsed factor.

        Factor comes in two types:
        1. '<factor name><factor total level>'
        2. '<factor name><factor total level>(代表<factor main level>)'

        Format of parsed factor:
        {
            'name': <factor name>,
            'type': <factor type>,
            'total_level': <total level of this factor, main uma's and parents' combined>,
            'main_level': <main uma's level of this factor>
        }

        Args:
            factor_string: A string.
        '''
        factor = {}

        s = factor_string.split('(代表')
        factor_name = s[0][:-1]
        factor['name'] = factor_name
        factor_type = self._get_factor_type(factor_name)
        factor['type'] = factor_type
        total_level = int(s[0][-1])
        factor['total_level'] = total_level
        if len(s) == 2:
            main_level = int(s[1][0])
            factor['main_level'] = main_level

        return factor

    def _get_factor_type(self, factor_name):
        '''Returns type of factor.

        Notice that the default type is race.
        If game database is not up to date,
        some skills might wrongly be classified as race

        '''
        if factor_name in self._BLUES:
            return 'blue'
        elif factor_name in self._FIELD_TYPES:
            return 'field_type'
        elif factor_name in self._DISTANCES:
            return 'distance'
        elif factor_name in self._STRATEGIES:
            return 'strategy'
        elif factor_name == 'URAシナリオ':
            return 'ura'
        else:
            skill_id, skill_is_unique = \
                self._find_skill_id_and_uniqueness_by_name(factor_name)
            if skill_id is not None:
                if skill_is_unique:
                    return 'unique_skill'
                return 'common_skill'
            return 'race'

    def _guess_parents_by_unique_skill_factors(self, unique_skill_factors, main_uma_id):
        '''Returns list of dicts of parents.

        Uses unique skills to guess parents.
        '''
        parents = []
        for factor in unique_skill_factors:
            factor_name = factor['name']
            skill_id, _ = self._find_skill_id_and_uniqueness_by_name(factor_name)
            uma_id = self._find_uma_id_by_unique_skill(skill_id)
            if uma_id == main_uma_id:
                continue
            parent = {
                'id': uma_id,
                'factors': factor
            }
            parents.append(parent)
        return parents

    def _find_skill_id_and_uniqueness_by_name(self, skill_name):
        '''Finds skill that matches given name.

        Args:
            skill_name: A string.

        Returns:
            Pair of (skill_id: string, uniqueness: bool)
            Note that the id returned is the id already presented in the
            original urarawin database, rather than the _id (MongoDB ObjectId)
            assigned when inserted into our database.
            If skill is not found, returns (None, False).
        '''
        cache = self._cache['find_skill_by_name']
        if skill_name in cache:
            return cache[skill_name]

        collection = self._game_data_database['skills']
        skill = collection.find_one(
            {'name': skill_name},
            {'_id': 0, 'id': 1, 'rare': 1}
        )
        if skill is None:
            skill_id = None
            skill_is_unique = False
        else:
            skill_id = skill['id']
            skill_is_unique = skill['rare'] == '固有'

        cache[skill_name] = (skill_id, skill_is_unique)
        return skill_id, skill_is_unique

    def _find_uma_id_by_unique_skill(self, skill_id):
        '''Returns uma id who has the skill, otherwise returns None.

        Note that the id used is the id already presented in the original
        urarawin database, rather than the _id (MongoDB ObjectId) assigned when
        inserted into our database.

        Args:
            skill_id: A string representing the id of a unique skill.
        '''
        cache = self._cache['find_uma_by_unique_skill']
        if skill_id in cache:
            return cache[skill_id]

        collection = self._game_data_database['players']
        uma = collection.find_one(
            {'uniqueSkillList': skill_id},
            {'_id': 0, 'id': 1}
        )
        if uma is None:
            uma_id = None
        else:
            uma_id = uma['id']

        cache[skill_id] = uma_id
        return uma_id

    def _find_race_id_by_name(self, race_name):
        '''Finds race that matches given name.

        Args:
            race_name: A string.

        Returns:
            A string of the id of the matching race.
            Note that the id returned is the id already presented in the
            original urarawin database, rather than the _id (MongoDB ObjectId)
            assigned when inserted into our database.
            Returns None if race is not found.
        '''
        cache = self._cache['find_race_by_name']
        if race_name in cache:
            return cache[race_name]

        collection = self._game_data_database['races']
        race = collection.find_one(
            {'name': race_name},
            {'_id': 0, 'id': 1}
        )
        if race is None:
            race_id = None
        else:
            race_id = race['id']

        cache[race_name] = race_id
        return race_id
