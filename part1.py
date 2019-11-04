#!/usr/bin/env python
# coding: utf-8

import sys
from _ctypes import Array

import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from csv import reader
from pyspark.sql import types

from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import json


def output(metadata,_sc, table_name ):
    results = {
        "dataset_name": table_name,
        "columns":metadata,
        "key_column_candidates": ["test1", "test2"]
    }
   # print(results)

def profile(data,_sc, sqlContext, table_name):
    results = []
    for i in range(0,len(data.columns)):
        colName = data.columns[i]
        print(colName)
        query = "select distinct %s from %s " %(colName, table_name)
        temp = sqlContext.sql(query)
        null_count = temp.filter(temp[0].isNull()).count()
        non_empty = temp.count() - null_count
        temp_col = {
            "column_name": colName,
            "number_non_empty_cells": non_empty,
            "number_empty_cells": null_count,
            "number_distinct_values": temp.distinct().count(),
            "frequent_values": "null",
            "data_types": ["test1","test2"]
        }
        results.append(temp_col)
    return results


def extractMeta(_sc, sql):
    metaSchema = StructType([StructField("UniqueKey", StringType(), True),  # LongType
                             StructField("CreatedDate", StringType(), True),  # TimestampType
                             StructField("ClosedDate", StringType(), True),  # TimestampType
                             StructField("Agency", StringType(), True),
                             StructField("AgencyName", StringType(), True),
                             StructField("ComplaintType", StringType(), True),
                             StructField("Descriptor", StringType(), True),
                             StructField("LocationType", StringType(), True),
                             StructField("IncidentZip", StringType(), True),
                             StructField("IncidentAddress", StringType(), True),
                             StructField("StreeName", StringType(), True),
                             StructField("CrossStreet1", StringType(), True),
                             StructField("CrossStreet2", StringType(), True),
                             StructField("IntersectionStreet1", StringType(), True),
                             StructField("IntersectionStreet2", StringType(), True),
                             StructField("AddressType", StringType(), True),
                             StructField("City", StringType(), True),
                             StructField("Landmark", StringType(), True),
                             StructField("FacilityType", StringType(), True),
                             StructField("Status", StringType(), True),
                             StructField("DueDate", StringType(), True),  # TimestampType
                             StructField("ResolutionDescription", StringType(), True),
                             StructField("ResolutionActionUpdatedDate", StringType(), True),  # TimestampType
                             StructField("Community", StringType(), True),
                             StructField("BBL", StringType(), True),
                             StructField("Borough", StringType(), True),
                             StructField("X", StringType(), True),  # IntegerType
                             StructField("Y", StringType(), True),  # IntegerType
                             StructField("OpenDataChannelType", StringType(), True),
                             StructField("ParkFacilityName", StringType(), True),
                             StructField("ParkBorough", StringType(), True),
                             StructField("VehicleType", StringType(), True),
                             StructField("TaxiCompanyBorough", StringType(), True),
                             StructField("TaxiPickupLocation", StringType(), True),
                             StructField("BridgeHighwayName", StringType(), True),
                             StructField("BridgeHighwayDirection", StringType(), True),
                             StructField("RoadRamp", StringType(), True),
                             StructField("BridgeHighwaySegment", StringType(), True),
                             StructField("Latitude", StringType(), True),  # DoubleType
                             StructField("Longitude", StringType(), True),  # DoubleType
                             StructField("Location", StringType(), True)
                             ])
    data = _sc.read.csv(path=sys.argv[1],sep='\t', header=True, inferSchema=True)
    for col in range(0,len(data.columns)):
        data=data.withColumnRenamed(data.columns[col],data.columns[col].replace(" ",""))
    data.printSchema()
    table_name = "ThreeOneOne"
    data.createOrReplaceTempView(table_name)
    metadata = profile(data,_sc, sql, table_name)
    output(metadata,_sc, table_name)



if __name__ == "__main__":
    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    extractMeta(spark, sqlContext)

    # get command-line arguments
    inFile = sys.argv[1]

    # Enter your modules here
    sc.stop()
