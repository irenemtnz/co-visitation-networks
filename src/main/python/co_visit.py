from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
import count, unique


def covisit(input_df, rate, min_count, unique_b):
    # Duplicate domain column to perform self-join
    input2_df = input_df.withColumnRenamed('domain', 'domain2')
    # joined_df = self-join to get all domain combinations with same ip
    joined_df = (input_df.join(input2_df, 'ip', 'inner')
                 .where(input_df.domain != input2_df.domain2)
                 .drop('useragent', 'ssp', 'uuid')
                 # unique dataframe - TO DO: Make sure this works with tests
                 .drop_duplicates(subset=('domain', 'domain2', 'ip')))

    # count ips that have visited both domains
    count_df = (joined_df.groupBy(['domain', 'domain2']).count()
                .withColumnRenamed('count', 'covisit'))
    count_df = count_df.where(count_df.covisit > 1)

    if unique_b == False:
        tuples_df = count.count_visits(input_df)
        count_df = count_df.join(tuples_df, 'domain', 'inner').withColumnRenamed('count', 'visits')
    else:
        unique_df = unique.unique_tuples(input_df)
        count_df = count_df.join(unique_df, 'domain', 'inner').withColumnRenamed('count', 'visits')

    # calculate covisitation rate
    count_df = count_df.withColumn('covisit', col('covisit') / col('visits'))

    count_df = count_df.where(count_df.covisit > rate).where(count_df.visits > min_count)
    count_df = count_df.drop('visits')

    return count_df