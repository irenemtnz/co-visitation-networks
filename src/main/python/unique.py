# unique_tuples leaves one repetition of each tuple {domain, ip}
def unique_tuples(df):
    unique_df = df.drop_duplicates(subset=('domain', 'ip')).drop('time')
    return unique_df

def count_unique_tuples(df):
    unique_df = df.drop_duplicates(subset=('domain', 'ip')).drop('timestamp', 'useragent', 'ssp', 'uuid')
    unique_df = unique_df.groupby(['domain']).count()
    unique_df = unique_df.orderBy('count', ascending = False)
    return unique_df