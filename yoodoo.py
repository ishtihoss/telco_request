'''
yoodoo request
'''

# import functions
from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType


# ingest household id data

hid_path = "s3a://ada-platform-components/Household/household/MY/restrict_ip/2020-06-01_2020-06-30/*"
hid_df  = spark.read.parquet(hid_path)

# ingest telco market insight data

tmi_path = 's3a://ada-geogrid-feature-set/telco_market_insight/high_quality/MY/2021*'
tmi_df = spark.read.parquet(tmi_path).select('ifa','cellular_carriers')

# join on ifa

jdf = hid_df.join(tmi_df, on='ifa', how='inner')

# explode the carriers column

jdfx = jdf.withColumn('carriers',F.explode('cellular_carriers')).drop('cellular_carriers')


# groupby and collect list

jdf1 = jdfx.groupby('component').agg(F.collect_list('carriers'))
jdf1.take(5)
