# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 14:52:58 2022

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
aws_secret_access_key = "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"
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
my_query = "ALTER TABLE forecast120 RENAME TO forecast120_{}".format(now.strftime('%Y%m%d'))
connection.execute(my_query)

df.to_sql('forecast120', engine)     

class Tenth:
  def __init__(self, start, end, tenthIndex):
    self.start = start
    self.end = end
    self.tenthIndex = tenthIndex
    
def getTenthIndexByDate(startTenth):
    dateTime =  datetime(startTenth.year, 1,1)
    monthList = []
    for i in range (0, 12):
        monthList.append(dateTime + relativedelta(months=i))
    
    num = 0
    for monthFisrtDay in monthList:
        
        dateTime2 = monthFisrtDay
        num2 =  monthrange(monthFisrtDay.year, monthFisrtDay.month)
        
        num+=1
        dateTime = dateTime2
        dateTime2 = dateTime2 + timedelta(days=9)
        t = dateTime2
        if (startTenth>= dateTime and startTenth <= t):
            return num
        
        num+=1
        dateTime2 = dateTime2 + timedelta(days=1)
        dateTime = dateTime2
        dateTime2 =  dateTime2 + timedelta(days=9)
        t = dateTime2
        if (startTenth>= dateTime and startTenth <= t):
            return num
        
        num+=1
        dateTime2 = dateTime2 + timedelta(days=1)
        dateTime = dateTime2
        dateTime2 = dateTime2 + timedelta(days=(num2[1] - dateTime2.day))
        t = dateTime2
        if (startTenth>= dateTime and startTenth <= t):
            return num                                     
    
    return -1

datesList =  []
totalDays = 180

startDate = datetime(now.year,now.month,1)
endDate = startDate + timedelta(days=totalDays)     
months = totalDays//(365.25/12)

for i in range(0, int(months)):
    datesList.append(startDate + relativedelta(months=i))
    
tenthPeriodList = []
for tableMonth in datesList:
    
    atualTenthPeriod = datetime(tableMonth.year,tableMonth.month,tableMonth.day)
    days = monthrange(tableMonth.year, tableMonth.month)
    
    #TENTH 1
    startTenth = atualTenthPeriod
    atualTenthPeriod = atualTenthPeriod + timedelta(days=9)
    endTenth = atualTenthPeriod
    indexTenth = getTenthIndexByDate(startTenth)
    tenthPeriod = Tenth(startTenth, endTenth, indexTenth)
    tenthPeriodList.append(tenthPeriod)
    
    #TENTH 2
    atualTenthPeriod = atualTenthPeriod + timedelta(days=1)
    startTenth = atualTenthPeriod
    atualTenthPeriod = atualTenthPeriod + timedelta(days=9)
    endTenth = atualTenthPeriod
    indexTenth = getTenthIndexByDate(startTenth)
    tenthPeriod = Tenth(startTenth, endTenth, indexTenth)
    tenthPeriodList.append(tenthPeriod)
    
    #TENTH 3
    atualTenthPeriod = atualTenthPeriod + timedelta(days=1)
    startTenth = atualTenthPeriod
    atualTenthPeriod = atualTenthPeriod + timedelta(days=(days[1] - atualTenthPeriod.day))
    endTenth = atualTenthPeriod
    indexTenth = getTenthIndexByDate(startTenth)
    tenthPeriod = Tenth(startTenth, endTenth, indexTenth)
    tenthPeriodList.append(tenthPeriod)
    

q1 = """SELECT distinct(geo) FROM df """

geoHash = ps.sqldf(q1, locals())

yearList = set()
for tenthPeriod in tenthPeriodList:
    yearList.add(tenthPeriod.start.year)
yearList = list(yearList)

for year in yearList:
    
    prcpYearDf = pd.DataFrame()
    tempYearDf = pd.DataFrame()
    count = 0
    for h3Cell in geoHash["geo"]:    
        
        print('Start processing for {}'.format(h3Cell))
        
        prcpCellDict = {"hash_grid":h3Cell}
        tempCellDict = {"hash_grid":h3Cell}
        
        for tenthPeriod in tenthPeriodList:
            dateRange = pd.date_range(tenthPeriod.start,tenthPeriod.end)
            dateRange = dateRange.format(formatter=lambda x: x.strftime('%Y-%m-%d'))  
            
            print('Processing dates from {} to {}'.format(tenthPeriod.start.strftime('%Y-%m-%d'),tenthPeriod.end.strftime('%Y-%m-%d')))
            
            rows = "('" + "','".join(dateRange) + "')"
             
            query = "SELECT prcp, temp FROM df WHERE ds IN " + rows + " AND geo = '" + h3Cell + "';"
             
            dataSet = ps.sqldf(query, locals())
            prcpAcumulate = 0
            tempAcumulate = 0
             #rowsArray = len(dateRange)
            for row in dataSet.iterrows():
                prcpAcumulate += row[1]["prcp"]
                tempAcumulate += row[1]["temp"]
             
            # prcpAverage = {"tenth_" + str(tenthPeriod) : prcpAcumulate/len(dateRange)}
            # tempAverage = {"tenth_" + str(tenthPeriod) : tempAcumulate/len(dateRange)}
            prcpCellDict["tenth_" + str(tenthPeriod.tenthIndex)] = prcpAcumulate/len(dateRange)
            tempCellDict["tenth_" + str(tenthPeriod.tenthIndex)] = tempAcumulate/len(dateRange)
        
        prcpYearDf = prcpYearDf.append(prcpCellDict, ignore_index=True)
        tempYearDf = tempYearDf.append(tempCellDict, ignore_index=True)
        count += 1
        
        print("Status : {}% concluded".format(str(100*count/len(geoHash))))

       
        
    prcpYearDf.to_csv(r"F:\Forecast\weather2020\prcpResult.csv", sep = ";")
    tempYearDf.to_csv(r"F:\Forecast\weather2020\tempResult.csv", sep = ";")
        
        
        
   
        
         

    
