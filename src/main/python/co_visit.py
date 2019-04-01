from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
import count


def co_visit(input_df):
    input2_df = input_df.withColumnRenamed('domain', 'domain2')
    joined_df = (input_df.join(input2_df, 'ip', 'left')
                 .where(input_df.domain != input2_df.domain2)
                 .drop_duplicates(subset=('domain', 'domain2', 'ip'))
                 .drop('time'))

    output_df = joined_df.crosstab("domain", "domain2").withColumnRenamed("domain_domain2", "domain")

    count_df = count.count_by_domain(input_df)

    covisit_df = output_df.join(count_df, 'domain', 'inner')

    for field in covisit_df.columns:
        if field != 'domain':
            covisit_df = covisit_df.withColumn(field, col(field) / col('count'))

    covisit_df = covisit_df.drop('count')
    return covisit_df
