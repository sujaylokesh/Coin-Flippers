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
        "columns": metadata,
        "key_column_candidates": ["test1", "test2"]
    }
    print(results)

def profile(data,_sc, sqlContext, table_name):
    results = []
    for i in range(0, 1):
        colName = data.columns[i]
        print(colName)
        query = "select distinct %s from %s " % (colName, table_name)
        temp = sqlContext.sql(query)
        null_count = temp.filter(temp[0].isNull()).count()
        non_empty = temp.count() - null_count
        distinct_count = temp.distinct().count()
        query = "select %s as val, count(*) as cnt from %s group by %s order by cnt desc" % (colName, table_name, colName)
        top5 = sqlContext.sql(query)
        top5 = top5.rdd.map(lambda x: x[0]).take(5)
        temp_col_metadata = {
            "column_name": colName,
            "number_non_empty_cells": non_empty,
            "number_empty_cells": null_count,
            "number_distinct_values": distinct_count,
            "frequent_values": top5,
            "data_types": ["test1","test2"]
        }
        results.append(temp_col_metadata)
    return results


def extractMeta(_sc, sql):
    data = _sc.read.csv(path=sys.argv[1],sep='\t', header=True, inferSchema=True)
    for col in range(0,len(data.columns)):
        data = data.withColumnRenamed(data.columns[col],
                                      data.columns[col].replace(" ","")
                                      .replace("(", "")
                                      .replace(")", ""))
    data.printSchema()
    table_name = "ThreeOneOne"
    data.createOrReplaceTempView(table_name)
    metadata = profile(data,_sc, sql, table_name)
    output(metadata,_sc, table_name)

def integerStatistics(_sc,column):

    result = {
        "type": "INTEGER",
        "count": column.count(),
        "max_value": column.max(),
        "min_value": column.min(),
        "mean": column.mean(),
        "stddev":column.std()
    }

    return result

def dateStatistics(_sc, column):

    df = spark.createDataFrame(column, "string").selectExpr("CAST(value AS date) AS date")
    min_date, max_date = df.select(min("date"), max("date")).first()

    result = {
        "type": "DATE/TIME",
        "count": column.count(),
        "max_value" : max_date,
        "min_value" : min_date
    }

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
