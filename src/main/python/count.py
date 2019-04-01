# count_tuples groups by domain and ip
def count_tuples(df):
    count_df = df.groupby(['domain', 'ip']).count()
    return count_df


# count groups all visits by domain
def count_by_domain(df):
    count_df = df.groupby(['domain']).count()
    return count_df


# count_visits groups all visits by domain, differencing by IP
def count_visits(df):
    count_df = df.groupby(['domain']).pivot('ip').count()
    return count_df
