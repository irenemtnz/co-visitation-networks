from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import string
import re
# import pandas as pd
# import numpy as np
from tld import get_tld
WWW = re.compile('^www\.')


# def main():
#     spark = ( SparkSession
#                       .builder
#                       .master("local")
#                       .appName("functions")
#                       .getOrCreate())

#     input_df = (spark.createDataFrame([['www.google.com', 'x', '3'],
#                                                 ['Youtube.com', 'y', '4'],
#                                                 ['http://www.google.com', 'x', '5'],
#                                                 ['www.youtube.com', 'x', '6'] ],
#                                                ['domain', 'ip', 'time']))
#     udfParseDomain = udf(parse_domain, StringType())
#     output_df = input_df.withColumn('domain', udfParseDomain('domain')).toPandas()
#     expected_df = (pd.DataFrame(np.array([['google.com', 'x', '3'],
#                                               ['youtube.com', 'y', '4'],
#                                               ['google.com', 'x', '5'],
#                                               ['youtube.com', 'x', '6']]),
#                                     columns=['domain', 'ip', 'time']))
#     print(output_df)
#     print(expected_df)

def parse_domain(domain) :
    if not domain:
        return domain
    domain = domain.lower() # force lowercase to use binary collate in the db
    # app ids come clean
    if domain.isdigit():
        return domain
    if domain.startswith('com.'):
        return domain.split('&',1)[0]
    domain = ''.join(filter(string.printable.__contains__, domain)) # remove non printable characters
    domain_cleaned = WWW.subn('',domain.replace('"','').strip().split('//',1)[-1].split('/',1)[0],count=1)[0]
    # return cleaned top level domain or discard
    try:
        return get_fld("http://" + domain_cleaned )
    except:
        return domain_cleaned
    
# main()
