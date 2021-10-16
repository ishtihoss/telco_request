'''
yoodoo request
'''
# ingest household id data

hid_path = "s3a://ada-platform-components/Household/household/MY/restrict_ip/2020-06-01_2020-06-30/*"
hid_df  = spark.read.parquet(hid_path)

# ingest telco market insight data

tmi_path = 's3a://ada-geogrid-feature-set/telco_market_insight'
