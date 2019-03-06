from pyspark.sql import SparkSession

#import pandas as pd
#import numpy as np

# def main():
#     spark = (SparkSession
#         .builder
#         .master("local")
#         .appName("test_functions")
#         .getOrCreate())

#     df = (spark.createDataFrame([ ['A', 'x', '3'], 
#                                    ['B', 'y', '4'], 
#                                    ['A', 'x', '5'],
#                                    ['B', 'x', '6'] ],
#                                    ['domain', 'ip', 'time']) )
#     result = (pd.DataFrame(np.array([ ['A', 'x'], 
#                                        ['B', 'y'],
#                                        ['B', 'x'] ]),
#                              columns=['domain', 'ip']) )
#     df = unique_tuples(df)
#     df.show()
#     print(result)


def unique_tuples(df):
    unique_df = df.drop_duplicates(subset=('domain', 'ip')).drop('time')
    return unique_df

# main()
