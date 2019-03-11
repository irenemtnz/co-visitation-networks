import unittest
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tld import get_tld
import unique
import count
import domain_parse


class Test(unittest.TestCase):

    spark = (SparkSession
             .builder
             .master("local")
             .appName("test_functions")
             .getOrCreate()
            )

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

    def test_parse_domain(self):
        input_df = (Test.spark.createDataFrame([['www.google.com', 'x', '3'],
                                                ['Youtube.com', 'y', '4'],
                                                ['http://www.google.com', 'x', '5'],
                                                ['www.youtube.com', 'x', '6'] ],
                                               ['domain', 'ip', 'time']))
        udfParseDomain = udf(domain_parse.parse_domain, StringType())
        output_df = input_df.withColumn("domain", udfParseDomain("domain")).toPandas()
        expected_df = (pd.DataFrame(np.array([['google.com', 'x', '3'],
                                              ['youtube.com', 'y', '4'],
                                              ['google.com', 'x', '5'],
                                              ['youtube.com', 'x', '6']]),
                                    columns=['domain', 'ip', 'time']))
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'ip', 'time'])
        
    def test_domain_unique(self):
        input_df = (Test.spark.createDataFrame([['www.google.com', 'x', '3'],
                                                ['Youtube.com', 'y', '4'],
                                                ['http://www.google.com', 'x', '5'],
                                                ['www.youtube.com', 'x', '6']],
                                                ['domain', 'ip', 'time']))
        udfParseDomain = udf(domain_parse.parse_domain, StringType())
        input_df = input_df.withColumn("domain", udfParseDomain("domain"))
        output_df = unique.unique_tuples(input_df).toPandas()

        expected_df = (pd.DataFrame(np.array([['google.com', 'x'],
                                            ['youtube.com', 'y'],
                                            ['youtube.com', 'x']]),
                                            columns=['domain', 'ip']))
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'ip'])

    def test_domain_count(self):
        input_df = ( Test.spark.createDataFrame([ ['WWW.GOOGLE.COM', 'x', '3'],
                                       ['Youtube.com', 'y', '4'],
                                       ['google.com', 'x', '5'],
                                       ['http://youtube.com', 'x', '6'],
                                       ['www.gmail.com', 'z', '8'] ],
                                       ['domain', 'ip', 'time']) )
        udfParseDomain = udf(domain_parse.parse_domain, StringType())
        input_df = input_df.withColumn("domain", udfParseDomain("domain"))
        output_df = count.count_tuples(input_df).toPandas()

        expected_df = ( pd.DataFrame(np.array([ ['google.com', 'x', '2'],
                                       ['youtube.com', 'x', '1'],
                                       ['youtube.com', 'y', '1'],
                                       ['gmail.com', 'z', '1'] ]),
                                       columns=['domain', 'ip', 'count']) )
        expected_df['count'] = expected_df['count'].astype(int)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'ip', 'count'])
    
    def test_domain_count_visit(self):
        input_df = (Test.spark.createDataFrame([['WWW.GOOGLE.COM', 'x', '3'],
                                       ['Youtube.com', 'y', '4'],
                                       ['google.com', 'x', '5'],
                                       ['http://youtube.com', 'x', '6'],
                                       ['www.gmail.com', 'z', '8']],
                                       ['domain', 'ip', 'time']))
        udfParseDomain = udf(domain_parse.parse_domain, StringType())
        input_df = input_df.withColumn("domain", udfParseDomain("domain"))
        output_df = count.count(input_df).toPandas()

        expected_df = ( pd.DataFrame(np.array([ ['google.com', '2'],
                                       ['youtube.com', '2'],
                                       ['gmail.com', '1'] ]),
                                       columns=['domain', 'count']) )
        expected_df['count'] = expected_df['count'].astype(int)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'count'])
    


def assert_frame_equal_with_sort(results, expected, keycolumns):
    expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    pd.testing.assert_frame_equal(results_sorted, expected_sorted, check_index_type=False)

if __name__ == '__main__':
    unittest.main()
