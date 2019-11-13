#!/usr/bin/env python
# coding: utf-8

import sys
from _ctypes import Array

import pyspark
import string
import statistics
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

def statistics(_sc,column):
    intList =[]
    dateList=[]
    txtList=[]
    datatype = set()
    res = {}

    for i in range(len(column)):
        typeElement = type(column[i])

        if(typeElement == int or typeElement == float ):
            intList.append(column[i])
            datatype.add("Integer/Real")
        elif(isinstance(column[i], datetime.date)):

            dateList.append(column[i])
            datatype.add("Date")
        elif(typeElement == str):
            txtList.append(column[i])
            datatype.add("Text")



    if len(intList) > 0:
        result = {
            "type": "INTEGER/REAL",
            "count": len(intList),
            "max_value": max(intList),
            "min_value": min(intList),
            "mean": statistics.mean(intList) ,
            "stddev": statistics.stdev(intList)
        }
        res.append(result)

    if len(dateList) > 0:
        result = {
            "type": "DATE/TIME",
            "count": len(dateList),
            # "max_value" : max_date,
            # "min_value" : min_date
        }
        res.append(result)
    

    #count number of integers in each word in a list
    templist = txtList
    counts = []
    max_values = []
    for txt in range(0,len(templist)):
        counts[txt] = templist[txt].count()
    for i in range(0,len(counts)):
        first = max(counts)
        counts.remove(first)
        second = max(counts)
        counts.remove(second)
        third = max(counts)
        counts.remove(third)
        fourth = max(counts)
        counts.remove(fourth)
        fifth = max(counts)
    max_values = [first,second,third,fourth,fifth]
    res.append(max_values)
 
    return res

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
