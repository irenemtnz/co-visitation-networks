from pyspark.sql.functions import col, least
import unique
import count

def covisit(input_df, rate, min_count, unique_b):
    # Duplicate domain column to perform self-join

    if unique_b is False:

        input2_df = count.count_tuples(input_df).withColumnRenamed('domain', 'domain2').withColumnRenamed('count', 'count2')

        # joined_df = self-join to get all domain combinations with same ip
        joined_df = (count.count_tuples(input_df).join(input2_df, 'ip', 'inner')
                     .where(input_df.domain != input2_df.domain2)
                     .drop('useragent', 'ssp', 'uuid'))

        count_df = joined_df.withColumn('number_covisitations', least('count', 'count2')).drop('count', 'count2')

        count_df = count_df.groupBy(['domain', 'domain2']).sum('number_covisitations')

        count_df = count_df.withColumnRenamed('sum(number_covisitations)', 'number_covisitations')

        count_df = count_df.join(count.count_by_domain(input_df), 'domain', 'inner') \
            .withColumnRenamed('count', 'visits')


    else:
        input2_df = input_df.withColumnRenamed('domain', 'domain2')

        #Drop all duplicates to get only one tuple for each domain-domain2-ip
        joined_df = (input_df.join(input2_df, 'ip', 'inner')
                    .where(input_df.domain != input2_df.domain2)
                    .drop('useragent', 'ssp', 'uuid')
                    .drop_duplicates(subset=('domain', 'domain2', 'ip')))

        count_df = (joined_df.groupBy(['domain', 'domain2']).count()
                    .withColumnRenamed('count', 'number_covisitations'))

        count_df = count_df.join(unique.count_unique_tuples(input_df), 'domain', 'inner') \
                    .withColumnRenamed('count', 'visits')


    # calculate co-visitation rate
    count_df = count_df.withColumn('covisit', col('number_covisitations') / col('visits'))
    count_df = count_df.where(count_df.covisit > rate).where(count_df.visits > min_count).drop('visits')

    count_df.show()

    return count_df




