import unittest
import pandas as pd
import numpy as np
import unique
import count
from pyspark.sql import SparkSession

class Test(unittest.TestCase):

    spark = (SparkSession
             .builder
             .master("local")
             .appName("test_functions")
             .getOrCreate())

    def test_unique(self):
        input_df = ( Test.spark.createDataFrame([ ['A', 'x', '3'], 
                                                 ['B', 'y', '4'], 
                                                 ['A', 'x', '5'],
                                                 ['B', 'x', '6'] ],
                                                 ['domain', 'ip', 'time']) )

        output_df = unique.unique_tuples(input_df).toPandas()

        expected_df = ( pd.DataFrame(np.array([ ['A', 'x'], 
                                                ['B', 'y'],
                                                ['B', 'x'] ]),
                                                columns=['domain', 'ip']) )
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'ip'])

    
    def test_count(self):
        input_df = ( Test.spark.createDataFrame([ ['A', 'x', '3'], 
                                   ['B', 'y', '4'], 
                                   ['A', 'x', '5'],
                                   ['B', 'x', '6'],
                                   ['C', 'z', '8'] ],
                                   ['domain', 'ip', 'time']) )
        output_df = count.count_tuples(input_df).toPandas()
        expected_df = ( pd.DataFrame(np.array([ ['A', 'x', '2'], 
                                       ['B', 'x', '1'],
                                       ['B', 'y', '1'],
                                       ['C', 'z', '1'] ]),
                                       columns=['domain', 'ip', 'count']) )
        expected_df['count'] = expected_df['count'].astype(int)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'ip', 'count'])


def assert_frame_equal_with_sort(results, expected, keycolumns):
    expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    pd.testing.assert_frame_equal(results_sorted, expected_sorted, check_index_type=False)

if __name__ == '__main__':
    unittest.main()