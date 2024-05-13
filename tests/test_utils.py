import unittest

from datetime import datetime, timezone
from aind_data_asset_indexer.utils import is_dict_corrupt

class TestUtils(unittest.TestCase):

    def test_is_dict_corrupt(self):
        good_contents = {"a": 1, "b": {"c": 2, "d": 3}}
        bad_contents1 = {"a.1": 1, "b": {"c": 2, "d": 3}}
        bad_contents2 = {"a": 1, "b": {"c": 2, "$d": 3}}

        self.assertFalse(is_dict_corrupt(good_contents))
        self.assertTrue(is_dict_corrupt(bad_contents1))
        self.assertTrue(is_dict_corrupt(bad_contents2))

    def test_does_object_exist(self):
        pass