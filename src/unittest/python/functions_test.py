import unittest
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import unique, count, domain_parse, co_visit, co_visit


class Test(unittest.TestCase):
    spark = (SparkSession
             .builder
             .master("local[2]")
             .appName("local testing")
             .getOrCreate())

    def test_unique(self):
        input_df = (Test.spark.createDataFrame([['A', 'x', '3'],
                                                ['B', 'y', '4'],
                                                ['A', 'x', '5'],
                                                ['B', 'x', '6']],
                                               ['domain', 'ip', 'time']))

        output_df = unique.unique_tuples(input_df).toPandas()

        expected_df = (pd.DataFrame(np.array([['A', 'x'],
                                              ['B', 'y'],
                                              ['B', 'x']]),
                                    columns=['domain', 'ip']))
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'ip'])

    def test_count(self):
        input_df = (Test.spark.createDataFrame([['A', 'x', '3'],
                                                ['B', 'y', '4'],
                                                ['A', 'x', '5'],
                                                ['B', 'x', '6'],
                                                ['C', 'z', '8']],
                                               ['domain', 'ip', 'time']))
        output_df = count.count_tuples(input_df).toPandas()

        expected_df = (pd.DataFrame(np.array([['A', 'x', '2'],
                                              ['B', 'x', '1'],
                                              ['B', 'y', '1'],
                                              ['C', 'z', '1']]),
                                    columns=['domain', 'ip', 'count']))
        expected_df['count'] = expected_df['count'].astype(int)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'ip', 'count'])

    def test_parse_domain(self):
        input_df = (Test.spark.createDataFrame([['www.google.com', 'x', '3'],
                                                ['Youtube.com', 'y', '4'],
                                                ['http://www.google.com', 'x', '5'],
                                                ['www.youtube.com', 'x', '6']],
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
        input_df = (Test.spark.createDataFrame([['WWW.GOOGLE.COM', 'x', '3'],
                                                ['Youtube.com', 'y', '4'],
                                                ['google.com', 'x', '5'],
                                                ['http://youtube.com', 'x', '6'],
                                                ['www.gmail.com', 'z', '8']],
                                               ['domain', 'ip', 'time']))
        udfParseDomain = udf(domain_parse.parse_domain, StringType())
        input_df = input_df.withColumn("domain", udfParseDomain("domain"))
        output_df = count.count_tuples(input_df).toPandas()

        expected_df = (pd.DataFrame(np.array([['google.com', 'x', '2'],
                                              ['youtube.com', 'x', '1'],
                                              ['youtube.com', 'y', '1'],
                                              ['gmail.com', 'z', '1']]),
                                    columns=['domain', 'ip', 'count']))
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
        output_df = count.count_by_domain(input_df).toPandas()

        expected_df = (pd.DataFrame(np.array([['google.com', '2'],
                                              ['youtube.com', '2'],
                                              ['gmail.com', '1']]),
                                    columns=['domain', 'count']))
        expected_df['count'] = expected_df['count'].astype(int)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'count'])

    def test_relation_visits(self):
        input_df = (Test.spark.createDataFrame([['A', 'x', '3'],
                                                ['A', 'y', '2'],
                                                ['A', 'w', '9'],
                                                ['A', 'x', '5'],
                                                ['B', 'x', '6'],
                                                ['B', 'x', '10'],
                                                ['B', 'y', '4'],
                                                ['C', 'w', '7'],
                                                ['C', 'z', '8']],
                                               ['domain', 'ip', 'time']))

        output_df = co_visit.covisit(input_df, 0.0, 0, False).toPandas()

        expected_df = (pd.DataFrame(np.array([['A', 'B', 0.5, 2],
                                              ['A', 'C', 0.25, 1],
                                              ['B', 'A', 1, 2],
                                              ['C', 'A', 0.5, 1]]),
                                    columns=['domain', 'domain2', 'covisit', 'visits']))

        expected_df['covisit'] = expected_df['covisit'].astype(float)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'domain2', 'covisit'])

    def test_all_visits_equal(self):
        input_df = (Test.spark.createDataFrame([['A', 'x', '3'],
                                                ['A', 'x', '2'],
                                                ['A', 'y', '9'],
                                                ['B', 'x', '6'],
                                                ['B', 'x', '10'],
                                                ['B', 'y', '4']],
                                               ['domain', 'ip', 'time']))

        output_df = co_visit.covisit(input_df, 0.0, 0, False).toPandas()

        expected_df = (pd.DataFrame(np.array([['A', 'B', 1.0, 3],
                                              ['B', 'A', 1.0, 3]]),
                                    columns=['domain', 'domain2', 'covisit', 'visits']))

        expected_df['covisit'] = expected_df['covisit'].astype(float)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'domain2', 'covisit'])

    def test_some_visits_repeated(self):
        input_df = (Test.spark.createDataFrame([['A', 'x', '1'],
                                                ['A', 'x', '2'],
                                                ['A', 'y', '3'],
                                                ['B', 'x', '4'],
                                                ['B', 'w', '5'],
                                                ['B', 'y', '6']],
                                               ['domain', 'ip', 'time']))

        output_df = co_visit.covisit(input_df, 0.0, 0, False).toPandas()

        expected_df = (pd.DataFrame(np.array([['A', 'B', 0.6, 2],
                                              ['B', 'A', 0.6, 2]]),
                                    columns=['domain', 'domain2', 'covisit', 'visits']))

        expected_df['covisit'] = expected_df['covisit'].astype(float)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'domain2', 'covisit'])

    def test_more_visits_A(self):
        input_df = (Test.spark.createDataFrame([['A', 'x', '1'],
                                                ['B', 'x', '2'],
                                                ['B', 'x', '3'],
                                                ['B', 'x', '4'],
                                                ['B', 'x', '5']],
                                               ['domain', 'ip', 'time']))

        output_df = co_visit.covisit(input_df, 0.0, 0, False).toPandas()

        expected_df = (pd.DataFrame(np.array([['A', 'B', 1.0, 1],
                                              ['B', 'A', 0.25, 1]]),
                                    columns=['domain', 'domain2', 'covisit', 'visits']))

        expected_df['covisit'] = expected_df['covisit'].astype(float)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'domain2', 'covisit'])

    def test_multiple_visits_both(self):
        input_df = (Test.spark.createDataFrame([['x', 'A', '10'],
                                          ['x', 'A', '20'],
                                          ['x', 'B', '11'],
                                          ['x', 'B', '21'],
                                          ['y', 'A', '30'],
                                          ['y', 'A', '40'],
                                          ['y', 'B', '31'],
                                          ['y', 'B', '41']],
                                         ['ip', 'domain', 'date_time']))

        output_df = co_visit.covisit(input_df, 0.0, 0, False).toPandas()

        expected_df = (pd.DataFrame(np.array([['A', 'B', 1.0, 4],
                                              ['B', 'A', 1.0, 4]]),
                                    columns=['domain', 'domain2', 'covisit', 'visits']))

        expected_df['covisit'] = expected_df['covisit'].astype(float)
        assert_frame_equal_with_sort(output_df, expected_df, ['domain', 'domain2', 'covisit'])

def assert_frame_equal_with_sort(results, expected, keycolumns):
    expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    pd.testing.assert_frame_equal(results_sorted, expected_sorted, check_index_type=False)


if __name__ == '__main__':
    unittest.main()
