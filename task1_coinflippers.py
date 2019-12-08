#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
from operator import add
import statistics
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import json

from pyspark.sql.types import DoubleType, IntegerType, FloatType

import FileInputManager as fm
import random
from dateutil import parser
import re


key_column_threshold = 10
output_mac_path = '/home/ml6543/project_final/output'
output_win_path = 'E:\\homework\\big_data\hw1\\project\\'
output_path = '/home/ml6543/project_final/output_task1'


def profileTable(data,_sc, sqlContext, table_name):
    results = []
    key_columns = []
    data_type = [0,0,0]
    print(table_name)
    for i in range(0,len(data.columns)):
        colName = fm.Process_column_name_for_dataframe(data.columns[i])
        col_type = data.schema.fields[i].dataType
        if (col_type == DoubleType() or \
                col_type == IntegerType() or\
                col_type == FloatType) and False:
            temp_results = profile_int_column(data, _sc, sqlContext, colName, table_name)
        elif does_col_contain_dates(_sc, data, colName, table_name):
            temp_results = profile_date_column()
        else:
            temp_results = profile_text_colum(_sc, sqlContext, colName, table_name)

        results.append(temp_results[0])

        if len(key_columns) > 0:
            key_columns.append(temp_results[1])
        data_type[0] += temp_results[2][0]
        data_type[1] += temp_results[2][1]
        data_type[2] += temp_results[2][2]
    return [results, data.selectkey_columns, data_type]

def does_col_contain_dates(_sc, data, colName, table_name):
    size = data.count()
    sample_count = 10
    sampled_rows = data.select(data[colName])\
        .sample(True, sample_count / size).collect()
    all_dates = True
    for r in sampled_rows:
        try:
            parser.parse()
        except:
            all_dates = False
            break
    return all_dates

def profile_date_column(data, _sc, sqlContext,colName, table_name):
    results = []
    total_count = data.select(data[colName]).count()
    empty_count = data.filter(data[colName].isNull()).count()
    distinct = data.select(data[colName]).distinct()
    distinct_count = distinct.count()
    query = "select %s as val, count(*) as cnt from %s group by val order by cnt desc" % (colName, table_name)
    top5 = sqlContext.sql(query)
    top5 = top5.rdd.map(lambda x: x[0]).take(5)
    temp_col_metadata = {
        "column_name": colName,
        "data_types": ["DATE", total_count - empty_count]
    }

    results.append(temp_col_metadata)

    key_columns = []
    diff = abs(total_count - empty_count - distinct_count)
    if diff < key_column_threshold:
        key_columns.append(colName)

    return results, key_columns, total_count - empty_count

def profile_int_column(data, _sc, sqlContext,colName, table_name):
    results = []
    list = data.collect()
    total_count = data.select(data[colName]).count()
    empty_count = data.filter(data[colName].isNull()).count()
    distinct = data.select(data[colName]).distinct()
    distinct_count = distinct.count()
    query = "select %s as val, count(*) as cnt from %s group by val order by cnt desc" % (colName, table_name)
    top5 = sqlContext.sql(query)
    top5 = top5.rdd.map(lambda x: x[0]).take(5)
    type_results = {}
    if total_count > 1:
        type_results = {
            "type": "INTEGER/REAL",
            "count": total_count,
            "max_value": top5[0],
            "min_value": top5.rdd.map(lambda x: x[0]).last(),
            "mean": statistics.mean(list),
            "stddev": statistics.stdev(list)
        }
    else:
        type_results = {
            "type": "INTEGER/REAL",
            "count": total_count,
            "max_value": top5[0],
            "min_value": top5.rdd.map(lambda x: x[0]).last(),
            "mean": statistics.mean(intList),
            "stddev": 0
        }

    temp_col_metadata = {
        "column_name": colName,
        "number_non_empty_cells": total_count - empty_count,
        "number_empty_cells": empty_count,
        "number_distinct_values": distinct_count,
        "frequent_values": top5,
        "data_types":
    }

    typeCount[0] = 1
    res.append(result)

    results.append(temp_col_metadata)

    key_columns = []
    diff = abs(total_count - empty_count - distinct_count)
    if diff < key_column_threshold:
        key_columns.append(colName)

    return results, key_columns, total_count - empty_count

def profile_text_colum(_sc, sqlContext, colName, table_name):
    results = []
    query = "select %s from %s" % (colName, table_name)
    temp = sqlContext.sql(query)
    # get data sets
    discinct_rows = temp.distinct()
    non_empty_rows = temp.filter(temp[0].isNull())
    null_count = non_empty_rows.count()
    non_empty = temp.count() - null_count
    print("here")
    distinct_count = discinct_rows.count()
    print("distinct_count",distinct_count)
    query = "select %s as val, count(*) as cnt from %s group by val order by cnt desc" % (colName, table_name)
    print("after query")
    top5 = sqlContext.sql(query)
    top5 = top5.rdd.map(lambda x: x[0]).take(5)
    data_type_stats, \
    typeCount = calc_statistics(_sc, discinct_rows)
    temp_col_metadata = {
        "column_name": colName,
        "number_non_empty_cells": non_empty,
        "number_empty_cells": null_count,
        "number_distinct_values": distinct_count,
        "frequent_values": top5,
        "data_types": data_type_stats
    }
    results.append(temp_col_metadata)
    key_columns = []
    diff = abs(non_empty - distinct_count)
    if diff < key_column_threshold:
        key_columns.append(colName)

    return results, key_columns, typeCount

def extractMeta(_sc, sqlContext, file_path, final_results):
    data = _sc.read.csv(path=file_path, sep='\t', header=True, inferSchema=False)
    col_size = len(data.columns)
    row_size = len(data.count())
    size_limit = 30000000
    if col_size * row_size > size_limit:
        return

    for col in range(0,len(data.columns)):
        data = data.withColumnRenamed(data.columns[col], fm.Process_column_name_for_dataframe(data.columns[col]))
    data.printSchema()
    delm = ""
    if file_path.find('/') > -1:
        delm = '/'
    else:
        delm = '\\'
    table_name = file_path.split(delm)[-1]
    print(table_name)
    dot_index = table_name.find(".")
    if dot_index == -1:
        dot_index = len(table_name)
    table_name = table_name[0: dot_index].replace("-", "_")
    data.createOrReplaceTempView(table_name)
    data = profileTable(data, _sc, sqlContext, table_name)
    col_metadata = data[0]
    key_col_candidate = data[1]
    data_type_count = data[2]
    #OUTPUT
    results = {
        "dataset_name": table_name,
        "columns": col_metadata,
        "key_column_candidates": key_col_candidate,
        "data_type_count": {"integer/real": data_type_count[0],
                            "date": data_type_count[1],
                            "text": data_type_count[2]}
    }
    final_results.append(results)
    sqlContext.dropTempTable(table_name)



# getting statistics based on data type of the elements in a column
def calc_statistics(_sc, discinct_rows):
    intList =[]
    txtList=[]
    date_count = 0
    res = []
    typeCount = [0,0,0]
    rows = discinct_rows.collect()

    max_int = -100000000000
    min_int = 1000000000000

    max_date = datetime.datetime.strptime("1/1/1900 12:00:00 AM", "%m/%d/%Y %H:%M:%S %p")
    min_date = datetime.datetime.strptime("12/31/9999 12:00:00 AM", "%m/%d/%Y %H:%M:%S %p")

    for i in range(len(rows)):
        val = str(rows[i][0])
        if val.isnumeric():
            intList.append(int(val))
            max_int = max(max_int, int(val))
            min_int = min(min_int, int(val))
        else:
            #check date
            try:
                temp_date = datetime.datetime.strptime(val, "%m/%d/%Y %H:%M:%S %p")
                max_date = max(max_date, temp_date)
                min_date = min(min_date, temp_date)
                date_count = date_count + 1
            except ValueError:
                if val != "None":
                    txtList.append(val)

    if len(intList) > 0:
        if len(intList)>1:
            result = {
                "type": "INTEGER/REAL",
                "count": len(intList),
                "max_value": max_int,
                "min_value": min_int,
                "mean": statistics.mean(intList),
                "stddev": statistics.stdev(intList)
            }
        else:
            result = {
                "type": "INTEGER/REAL",
                "count": len(intList),
                "max_value": max_int,
                "min_value": min_int,
                "mean": statistics.mean(intList),
                "stddev": 0
            }
        typeCount[0]=1
        res.append(result)

    if date_count > 0:
        result = {
            "type": "DATE/TIME",
            "count": date_count,
            "max_value": max_date.strftime("%Y/%m/%d %H:%M:%S"),
            "min_value": min_date.strftime("%Y/%m/%d %H:%M:%S")
        }
        res.append(result)
        typeCount[1]=1

    if len(txtList) > 0:
        templist = _sc.sparkContext.parallelize(txtList)
        sorted_list = templist.map(lambda x: len(x)).distinct().sortBy(lambda x: x, ascending=False)
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
        typeCount[2]=1

    return res, typeCount

if __name__ == "__main__":

    config = pyspark.SparkConf().setAll(
        [('spark.executor.memory', '8g'), ('spark.executor.cores', '5'), ('spark.cores.max', '5'),
         ('spark.driver.memory', '8g')])
    sc = SparkContext(conf=config)
    sc.addFile("FileInputManager.py")
    sc.addFile("task1_coinflippers.py")
    sc.addFile("task2_coinflippers.py")

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    fm.iterate_files_from_file_for_task1(sc, spark, sqlContext, sys.argv[1],
                                         int(sys.argv[2]))

    sc.stop()