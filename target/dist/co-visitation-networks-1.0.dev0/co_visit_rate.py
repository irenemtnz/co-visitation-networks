from pyspark.sql import SparkSession
import count
import unique
import pandas as pd
import numpy as np

spark = (SparkSession
         .builder
         .master("local")
         .appName("test_functions")
         .getOrCreate())

input_df = (spark.createDataFrame([['A', 'x', '3'],
                                   ['B', 'y', '4'],
                                   ['A', 'x', '5'],
                                   ['B', 'x', '6'],
                                   ['C', 'z', '8'],
                                   ['A', 'y', '2']],
                                  ['domain', 'ip', 'time']))

expected_df = (pd.DataFrame(np.array([['A', 'B', '1', '1'],
                                      ['B', 'A', '1', '2']]),
                            columns=['domain', 'domain2', '1in2', 'count']))

input2_df = input_df.withColumnRenamed('domain', 'domain2')

joined_df = (input_df.join(input2_df, 'ip', 'inner')
             .where(input_df.domain != input2_df.domain2)
             .drop_duplicates(subset=('domain', 'domain2', 'ip'))
             .drop('time'))

output_df2 = joined_df.crosstab("domain", "domain2")

output_df2.show()
