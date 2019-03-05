import pandas as pd
import numpy as np


# def main():
#     i = np.array([0, 1, 2])
#     df = ( pd.DataFrame(np.array([ ['A', 'x', '3'], 
#                                    ['B', 'y', '4'], 
#                                    ['A', 'x', '5'],
#                                    ['B', 'x', '6'] ]),
#                              columns=['domain', 'ip', 'time']) )
#     result = ( pd.DataFrame(np.array([ ['A', 'x'], 
#                                        ['B', 'y'],
#                                        ['B', 'x'] ]),
#                              columns=['domain', 'ip']) )
#     df = unique_tuples(df)
#     print(df)
#     print(result)


def unique_tuples(df):
    unique_df = df.drop_duplicates(subset=('domain', 'ip')).drop('time', axis = 1)
    return unique_df

# main()