from pyspark.sql import SparkSession

#import pandas as pd
#import numpy as np


# def main():
#     spark = (SparkSession
#         .builder
#         .master("local")
#         .appName("test_functions")
#         .getOrCreate())

#     df = ( spark.createDataFrame([ ['A', 'x', '3'], 
#                                    ['B', 'y', '4'], 
#                                    ['A', 'x', '5'],
#                                    ['B', 'x', '6'],
#                                    ['C', 'z', '8'] ],
#                                    ['domain', 'ip', 'time'] ))
#     result = ( pd.DataFrame(np.array([ ['A', 'x', 2], 
#                                        ['B', 'x', 1],
#                                        ['B', 'y', 1],
#                                        ['C', 'z', 1] ]),
#                                        columns=['domain', 'ip', 'count']))
#     result['count'] = result['count'].astype(int)
#     df = count_tuples(df)
#     df.show()
#     print(result)
    
    
def count_tuples(df):
    count_df = df.groupby(['domain','ip']).count()
    return count_df

# main()
