
class Unique:

    # unique_tuples leaves one repetition of each tuple {domain, ip}
    def unique_tuples(df):
        unique_df = df.drop_duplicates(subset=('domain', 'ip')).drop('time')
        return unique_df

