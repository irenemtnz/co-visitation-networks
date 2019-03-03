import unittest
import unique
import count
import pandas as pd
import numpy as np

class TestUnique(unittest.TestCase):
    
    def test_unique(self):
        i = np.array([0, 1, 2])
        df = ( pd.DataFrame(np.array([ ['A', 'x', '3'], 
                                   ['B', 'y', '4'], 
                                   ['A', 'x', '5'],
                                   ['B', 'x', '6'] ]),
                                   columns=['domain', 'ip', 'time']) )

        result = unique.unique_tuples(df)
        expected = ( pd.DataFrame(np.array([ ['A', 'x'], 
                                       ['B', 'y'],
                                       ['B', 'x'] ]),
                             columns=['domain', 'ip']) )
        assert_frame_equal_with_sort(result, expected, ['domain', 'ip'])

    
    def test_count(self):
        i = np.array([0, 1, 2])
        df = ( pd.DataFrame(np.array([ ['A', 'x', '3'], 
                                   ['B', 'y', '4'], 
                                   ['A', 'x', '5'],
                                   ['B', 'x', '6'],
                                   ['C', 'z', '8'] ]),
                                   columns=['domain', 'ip', 'time']) )
        result = count.count_tuples(df)
        expected = ( pd.DataFrame(np.array([ ['A', 'x', '2'], 
                                       ['B', 'x', '1'],
                                       ['B', 'y', '1'],
                                       ['C', 'z', '1'] ]),
                                       columns=['domain', 'ip', 'count']) )
        expected['count'] = expected['count'].astype(int)
        assert_frame_equal_with_sort(result, expected, ['domain', 'ip', 'count'])


def assert_frame_equal_with_sort(results, expected, keycolumns):
    expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    pd.testing.assert_frame_equal(results_sorted, expected_sorted, check_index_type=False)

if __name__ == '__main__':
    unittest.main()