class Count:

    # count_tuples groups by domain and ip
    def count_tuples(self):
        count_df = self.groupby(['domain', 'ip']).count()
        return count_df

    # count groups all visits by domain
    def count_by_domain(self):
        count_df = self.groupby(['domain']).count()
        return count_df

    # count_visits groups all visits by domain, differencing by IP
    def count_visits(self):
        count_df = self.groupby(['domain']).pivot('ip').count()
        return count_df
