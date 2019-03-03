import pandas as pd
import numpy as np


# def main():
#     df = ( pd.DataFrame(np.array([ ['A', 'x', '3'], 
#                                    ['B', 'y', '4'], 
#                                    ['A', 'x', '5'],
#                                    ['B', 'x', '6'],
#                                    ['C', 'z', '8'] ]),
#                              columns=['domain', 'ip', 'time']) )
#     result = ( pd.DataFrame(np.array([ ['A', 'x', 2], 
#                                        ['B', 'x', 1],
#                                        ['B', 'y', 1],
#                                        ['C', 'z', 1] ]),
#                              columns=['domain', 'ip', 'count']))
#     result['count'] = result['count'].astype(int)
#     df = count_tuples(df)
    
    
def count_tuples(df):
    count_df = df.groupby(['domain','ip'])['domain'].agg('count').to_frame('count').astype(int).reset_index()
    return count_df

# main()