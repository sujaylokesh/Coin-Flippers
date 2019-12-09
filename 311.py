import time
import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.master("local").appName("DataCleaning311").getOrCreate()
data = spark.read.csv('311.csv',header=True,nullValue='Unspecified')
colData = []
#data.count()
length = data.count()
for columns in data.columns:
	colData.append(data.select([count(when((col(columns)=="NA") | (col(columns)=="Unspecified") | (col(columns)=="N/A") | (col(columns)=="") | (col(columns).isNull()) | (col(columns)=="0 Unspecified"),columns)).alias(columns)]).take(1)[0][0])

for i in range(0,len(colData)):
	colData[i]=(colData[i]/length)*100

headers=data.columns
for i in range(0,len(colData)):
	if(colData[i]>60):
		data=data.drop(headers[i])

data=data.withColumn("Closed Date",to_timestamp(col("Closed Date"),"M/d/y h:m:s a"))
data=data.withColumn("Created Date",to_timestamp(col("Created Date"),"M/d/y h:m:s a"))
data=data.withColumn("Resolution Action Updated Date",to_timestamp(col("Resolution Action Updated Date"),"M/d/y h:m:s a"))

data.createOrReplaceTempView("table")

data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Street.*","Street Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Highway.*","Highway Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Noise.*","Noise Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Taxi.*","Taxi Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Ferry.*","Ferry Complaint"))

data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10451) & (col("Incident Zip")<=10475),"BRONX").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=11201) & (col("Incident Zip")<=11239),"BROOKLYN").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10001) & (col("Incident Zip")<=10280),"MANHATTAN").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10301) & (col("Incident Zip")<=10314),"STATEN ISLAND").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=11354) & (col("Incident Zip")<=11697),"QUEENS").otherwise(col("Borough")))

data = data.drop('Park Facility Name')
data.createOrReplaceTempView("table")
data.toPandas().to_csv("clean.csv")
#data.coalesce(1).write.csv("Cleaned_311",header=True)