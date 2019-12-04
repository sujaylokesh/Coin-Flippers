#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
from _ctypes import Array
from operator import add

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
    for i in range(9,14):
        colName = data.columns[i]
        #data = data.collect()
        query = "select %s from %s" % (colName, table_name)
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
        print("semantics", semantics)
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
    txtList=[]
    date_count = 0
    res = []
    rows = discinct_rows.collect()

    max_int = -100000000000
    min_int = 1000000000000

    max_date = datetime.date(1900,1,1)
    min_date = datetime.date(9999,12,30)


    for i in range(len(rows)):
        typeElement = type(rows[i][0])
        if typeElement == int or typeElement == float:
            intList.append(rows[i][0])
            max_int = max(max_int, rows[i][0])
            min_int = max(min_int, rows[i][0])
        elif isinstance(rows[i][0], datetime.date):
            date_count = date_count + 1
            max_date = max(max_date, rows[i][0])
            min_date = max(min_date, rows[i][0])
        elif typeElement == str:
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
            "max_value": max_date,
            "min_value": min_date
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


if __name__ == "__main__":
    sc = SparkContext()
    #lines = sc.textFile("files/small.tsv")

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
