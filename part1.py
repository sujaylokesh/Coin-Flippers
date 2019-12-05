#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
from operator import add
import statistics
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import json
import task2_M as task2
from dateutil import parser
import re


key_column_threshold = 10
output_path = 'E:\\homework\\big data\hw1\project\\'


def output(metadata, key_columns, _sc, table_name ):
    results = {
        "dataset_name": table_name,
        "columns": metadata,
        "key_column_candidates": key_columns
    }
    print(results)
    path = "%s\\%s.json" % (output_path, table_name)
    with open(path, 'w') as json_file:
        json.dump(results, json_file)


def profileTable(data,_sc, sqlContext, table_name):
    results = []
    key_columns = []
    for i in range(0,len(data.columns)):
        colName = data.columns[i].replace(" ","").replace("(", "").replace(")", "")
        temp_results = profile_colum(_sc, sqlContext, colName, table_name)
        results.append(temp_results[0])
        key_columns.append(temp_results[1])
    return [results, key_columns]


def profile_colum(_sc, sqlContext, colName, table_name):
    results = []

    query = "select %s from %s" % (colName, table_name)
    temp = sqlContext.sql(query)
    # get data sets
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
    #   semantics = task2.semanticCheck(discinct_rows)
    # print("semantics", semantics)
    # results.append(semantics)
    # check if this column can be a keycolumn
    key_columns = []
    diff = abs(non_empty - distinct_count)
    if diff < key_column_threshold:
        key_columns.append(colName)

    return results, key_columns


def extractMeta(_sc, sqlContext, file_path):
    data = _sc.read.csv(path=file_path, sep='\t', header=True, inferSchema=True)
    table_name = file_path.split('\\')[-1]
    dot_index = table_name.find(".")
    table_name = table_name[0: dot_index]
    data.createOrReplaceTempView(table_name)
    data = profileTable(data, _sc, sqlContext, table_name)
    col_metadata = data[0]
    key_col_candidate = data[1]
    output(col_metadata, key_col_candidate,_sc, table_name)
    sqlContext.dropTempTable(table_name)



# getting statistics based on data type of the elements in a column
def calc_statistics(_sc, discinct_rows):
    intList =[]
    txtList=[]
    date_count = 0
    res = []
    rows = discinct_rows.collect()

    max_int = -100000000000
    min_int = 1000000000000

    max_date = datetime.datetime.strptime("1/1/1900 12:00:00 AM", "%m/%d/%Y %H:%M:%S %p")
    min_date = datetime.datetime.strptime("12/31/9999 12:00:00 AM", "%m/%d/%Y %H:%M:%S %p")

    for i in range(len(rows)):
        typeElement = type(rows[i][0])
        val = rows[i][0]
        if typeElement == int or typeElement == float:
            intList.append(val)
            max_int = max(max_int, val)
            min_int = max(min_int, val)
        elif typeElement == str:
            #check date
            try:
                temp_date = datetime.datetime.strptime(val, "%m/%d/%Y %H:%M:%S %p")
                max_date = max(max_date, temp_date)
                min_date = min(min_date, temp_date)
                date_count = date_count + 1
            except ValueError:
                txtList.append(rows[i][0])

    if len(intList) > 0:
        result = {
            "type": "INTEGER/REAL",
            "count": len(intList),
            "max_value": max_int,
            "min_value": min_int,
            "mean": statistics.mean(intList),
            "stddev": statistics.stdev(intList)
        }
        res.append(result)

    if date_count > 0:
        result = {
            "type": "DATE/TIME",
            "count": date_count,
            "max_value": max_date.strftime("%Y/%m/%d %H:%M:%S"),
            "min_value": min_date.strftime("%Y/%m/%d %H:%M:%S")
        }
        res.append(result)

    if len(txtList) > 0:
        templist = _sc.sparkContext.parallelize(txtList)
        sorted_list = templist.map(lambda x: len(x)).sortBy(lambda x: x, ascending=False)
        longest = sorted_list.take(5)
        shortest = sorted_list.take(5)
        count = templist.count()
        sum = templist.map(lambda x: len(x)).reduce(add)
        average = float(sum) / float(count)
        result = {
            "type": "TEXT",
            "count": len(txtList),
            "shortest_values": shortest,
            "longest_values": longest,
            "average_length": "%.f2" % average
        }
        res.append(result)
    return res

if __name__ == "__main__":
    sc = SparkContext()
    #lines = sc.textFile("files/small.tsv")

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    #task2.initialize()

    # get command-line arguments
    # files = []
    # for i in range(1, len(sys.argv)):
    #     files.append(sys.argv[i])
    #
    # for i in range(0, len(files)):
    #     extractMeta(spark, sqlContext, files[i])


    # Enter your modules here
    sc.stop()
