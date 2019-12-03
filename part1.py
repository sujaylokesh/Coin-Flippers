#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
from _ctypes import Array
from names_dataset import NameDataset
import pyspark
import string
import statistics
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from csv import reader
from pyspark.sql import types
import math
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import task2_M as task2


key_column_threshold = 10
output_path = 'E:\\homework\\big data\hw1\project\\filestestJsonFIle.json'

def output(metadata, key_columns, _sc, table_name ):
    results = {
        "dataset_name": table_name,
        "columns": metadata,
        "key_column_candidates": key_columns
    }
    print(results)
    with open(output_path, 'w') as json_file:
        json.dump(results, json_file)


def profile(data,_sc, sqlContext, table_name):
    results = []
    key_columns = []
    for i in range(0, 1):
        colName = data.columns[i]
        print(colName)
        query = "select %s from %s " % (colName, table_name)
        temp = sqlContext.sql(query)

        #get data sets
        discinct_rows = temp.distinct()
        non_empty_rows = temp.filter(temp[0].isNull())

        null_count = non_empty_rows.count()
        non_empty = temp.count() - null_count
        distinct_count = discinct_rows.count()

        query = "select %s as val, count(*) as cnt from %s group by %s order by cnt desc" % (colName, table_name, colName)
        top5 = sqlContext.sql(query)
        top5 = top5.rdd.map(lambda x: x[0]).take(5)
        temp_col_metadata = {
            "column_name": colName,
            "number_non_empty_cells": non_empty,
            "number_empty_cells": null_count,
            "number_distinct_values": distinct_count,
            "frequent_values": top5,
            "data_types": calc_statistics(_sc, discinct_rows)
        }
        results.append(temp_col_metadata)
        #### need updates for count
        semantics = task2.semanticCheck(discinct_rows)
        results.append(semantics)

        #check if this column can be a keycolumn
        diff = abs(non_empty - distinct_count)
        if diff < key_column_threshold:
            key_columns.append(colName)

        print(results)

    return [results, key_columns]


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
    data = profile(data,_sc, sql, table_name)
    col_metadata = data[0]
    key_col_candidate = data[1]
    output(col_metadata,key_col_candidate,_sc, table_name)

# getting statistics based on data type of the elements in a column
def calc_statistics(_sc, discinct_rows):
    intList =[]
    dateList=[]
    datatype=[]
    txtList=[]
    res = []
    rows = discinct_rows.collect()
    for i in range(len(rows)):
        typeElement = type(rows[i][0])
        if typeElement == int or typeElement == float:
            intList.append(rows[i][0])
            datatype.append("Integer/Real")
        elif isinstance(rows[i][0], datetime.date):
            dateList.append(rows[i][0])
            datatype.append("Date")
        elif typeElement == str:
            txtList.append(rows[i][0])
            datatype.append("Text")

    if len(intList) > 0:
        result = {
            "type": "INTEGER/REAL",
            "count": len(intList),
            "max_value": max(intList),
            "min_value": min(intList),
            "mean": statistics.mean(intList),
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
    max_values = {}
    if len(templist) > 0:
        for txt in range(0,len(templist)):
            counts[txt] = len(templist[txt])
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
        max_values ={
            "1st Highest": first,
            "2nd Highest": second,
            "3rd Highest": third,
            "4th Highest": fourth,
            "5th Highest": fifth
            }
        res.append(max_values)

    #Top 5 Shortest Values
    templist = txtList
    counts = []
    min_values = {}
    if len(templist) > 0:
        for txt in range(0,len(templist)):
            counts[txt] = templist[txt].count()
        for i in range(0,len(counts)):
            first = min(counts)
            counts.remove(first)
            second = min(counts)
            counts.remove(second)
            third = min(counts)
            counts.remove(third)
            fourth = min(counts)
            counts.remove(fourth)
            fifth = min(counts)
        min_values ={
            "1st Lowest": first,
            "2nd Lowest": second,
            "3rd Lowest": third,
            "4th Lowest": fourth,
            "5th Lowest": fifth
            }
        res.append(min_values)
    #Average Number
    templist = txtList
    counts = []
    avg = 0
    average = {}
    if len(templist) > 0:
        for txt in range(0,len(templist)):
            counts[txt] = templist[txt].count()
        avg = sum(counts)/len(templist)
        average = {"Average":avg}
        res.append(average)
    return res


if __name__ == "__main__":
    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    task2.initialize()

    extractMeta(spark, sqlContext)
    # get command-line arguments
    inFile = sys.argv[1]

    # Enter your modules here
    sc.stop()
    task2.checkNeiborhoods(0)
