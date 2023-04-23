import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from src.transform import identify_and_remove_duplicated_data


class TestIdentifyAndRemoveDuplicates(unittest.TestCase):
    def test_identify_and_removing_duplicates(self):
        d = pd.DataFrame({'col1': [1, 2, 1, 1], 'col2': [3, 4, 3, 3]})
        result = identify_and_remove_duplicated_data(d)
        expected_d = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        assert_frame_equal(result, expected_d)

    def test_no_duplicates(self):
        d = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        result = identify_and_remove_duplicated_data(d)
        assert_frame_equal(result, d)


if __name__ == '__main__':
    unittest.main()
