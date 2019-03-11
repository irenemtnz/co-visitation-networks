from pyspark.sql import SparkSession
import count


spark = (SparkSession
        .builder
        .master("local")
        .appName("test_functions")
        .getOrCreate())

input_df = (spark.createDataFrame([['A', 'x', '3'],
                                ['B', 'y', '4'],
                                ['A', 'x', '5'],
                                ['B', 'x', '6'],
                                ['C', 'z', '8'] ],
                                ['domain', 'ip', 'time'] ))
ips_df = count.count_visits(input_df)
count_df = count.count(input_df)
joined_df = ips_df.join(count_df, 'domain', 'inner')
joined_df.show()
