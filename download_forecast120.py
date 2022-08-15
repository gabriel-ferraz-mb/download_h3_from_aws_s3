# -*- coding: utf-8 -*-
"""
Created on Tue May 17 15:38:07 2022

@author: gabriel.ferraz
"""
import pandas as pd
# =============================================================================
# import h3
# import geojson
# import shapely.wkt
# import boto3
from boto3.session import Session
# import requests
# =============================================================================
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import pandasql as ps
# import psycopg2
from sqlalchemy import create_engine

global file_name

# DOWNLOAD NEW RELEASE
#=============================================================================
aws_access_key_id = "AKIAVAMLXVZE7SOMHVXX"
aws_secret_access_key = "key"
urlparquet ="s3://weather-delivery/agrotools/source/br-geos-only.snappy.parquet"
bucket = "weather-delivery"
prefix = "agrotools/forecasts"

session = Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)
s3 = session.resource('s3')
bucket = s3.Bucket(bucket)

for s3_file in bucket.objects.filter(Prefix=prefix):
    file_object = s3_file.key
    file_name = str(file_object.split('/')[-1])
    print('Downloading file {} ...'.format(file_object))
    bucket.download_file(file_object, r'F:\Forecast\weather2020/{}'.format(file_name))
#=============================================================================

# DOWNLOAD PARQUET
#=============================================================================
# dfParquet = pd.read_parquet(r'C:\ATPx\Forecast\br-geos-only.snappy.parquet', engine = "pyarrow")
# dfParquet["cell"] = h3.h3_to_string(dfParquet["geo"])
#=============================================================================

now = datetime.now()
# UPDLOAD DF
df = pd.read_csv( r'F:\Forecast\weather2020/' + file_name , sep = ",")
#df = pd.read_csv( r'C:\ATPx\Forecast\part-00000-f56ae8ef-4184-4ab8-bffa-f1ab8e830b02-c000.csv', sep = ",")

dbschema = "public"
password =  "dKQvblHToBzbZLZ"
engine = create_engine('postgresql://postgres:dKQvblHToBzbZLZ@localhost:5433/forecast', connect_args={'options': '-csearch_path={}'.format(dbschema)})
connection = engine.connect()
my_query = "ALTER TABLE IF EXISTS forecast120 RENAME TO forecast120_{}; DROP TABLE IF EXISTS forecast120;".format(now.strftime('%Y%m%d'))
try:
    connection.execute(my_query)
except Exception as e:  
    print(e)


df.head(0).to_sql('', engine, if_exists='replace',index=False)
df.to_sql('forecast120', engine)
print("Upload done at {}").format(now.strftime('%Y/%m/%d %H:%M:%S')) 
