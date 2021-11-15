'''
yoodoo request
'''

# import functions
from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

# ingest household id data

hid_path = 's3a://ada-platform-components/Household_v1.1/MY/household_id/smc_custom1/2021-09-01_2021-09-30/*'

hid_df  = spark.read.parquet(hid_path)

# ingest telco market insight data

tmi_path = 's3a://ada-geogrid-feature-set/telco_market_insight/high_quality/MY/2021*'
tmi_df = spark.read.parquet(tmi_path).select('ifa','cellular_carriers')

# join on ifa

jdf = hid_df.join(tmi_df, on='ifa', how='inner')

# explode the carriers column

jdfx = jdf.withColumn('carriers',F.explode('cellular_carriers')).drop('cellular_carriers')


# groupby and collect list

jdf1 = jdfx.groupby('household_id').agg(F.collect_list('carriers'))
jdf1 = jdf1.withColumnRenamed('collect_list(carriers)','carrier_list')


jdf2 = jdf1.filter(F.size('carrier_list') > 0)

# find out distinct list of carriers

#list = jdfx.select('carriers').distinct()

#carrier_list = list.select('carriers').rdd.flatMap(lambda x: x).collect()

# Create python function for UDF

def cel_per(carriers):
    return(len([1 for c in carriers if c=='Celcom'])/len(carriers))


# create UDF

cel_perUDF = F.udf(lambda x: cel_per(x), T.FloatType())

# create new column with counts

exp = jdf2.withColumn("count",cel_perUDF(F.col("carrier_list")))

# Identify households that are not using Celcom at all

non_cel = exp.filter(F.col('count') == 0)

# Identify households with majority celcom users (75 percent or higher)

maj_cel = exp.filter(F.col('count') >= 0.75)

#exp1 = exp.withColumn('percentage',F.col('count')/len(F.col('carrier_list')))

# Sample code for reference
#df.select(col("Seqno"), \
#    convertUDF(col("Name")).alias("Name") ) \
#   .show(truncate=False)

# scrap code (check new data source for hhid)

#path_q = 's3a://ada-platform-components/Household_v1.1/MY/household_id/smc_custom1/2021-09-01_2021-09-30/*'
#df_q = spark.read.parquet(path_q)


# Some sample code

#from pyspark.sql import functions as F
#from pyspark.sql import types as T
#from pyspark.sql import Window

#subtag_music = ["a life in music", "music for life", "bso", "hans zimmer"]

#def subcadena_en_vector(tags):
    #return(sum([1 for c in tags if "music" in c]))

#print(subcadena_en_vector(subtag_music))

#subtag_music_UDF = F.udf(subcadena_en_vector, T.IntegerType())
#videosOcurrenciasMusicDF = videosDiasViralDF.withColumn("ocurrencias_music", subtag_music_UDF(F.col("tags")))
