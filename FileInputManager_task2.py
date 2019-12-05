import sys
import datetime
from operator import add
import statistics
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import json
#import task2_M as task2
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
import task1_coinflippers as p1

dumbo_path = '/user/hm74/NYCOpenData/'

def strip_char(str):
    return str.replace('[', "")\
        .replace(']', "")\
        .replace("\\", "").replace("\'", "")

def customMap(file_name):
    fields = file_name.split('.')
    name = strip_char(file_name)
    table_name = strip_char(fields[0])
    col_name = strip_char(fields[1])
    return name, table_name, col_name

def getFilePathsFromFile(sc, path):
    data = sc.textFile(path)
    file_arrays = data.first().split(',')
    file_rdd = sc.parallelize(file_arrays)
    results = file_rdd.map(lambda x: customMap(x)).collect()
    return results

def extractMetaByColum(_sc, sqlContext, file_info):
    file_path = dumbo_path + file_info[0]
    data = _sc.read.csv(path=file_path, sep='\t', header=True, inferSchema=True)
    table_name = file_path.split('\\')[-1]
    dot_index = table_name.find(".")
    table_name = table_name[0: dot_index]
    data.createOrReplaceTempView(table_name)
    data = p1.profile_colum(data, _sc, sqlContext, table_name)
    col_metadata = data[0]
    key_col_candidate = data[1]
    p1.output(col_metadata, key_col_candidate, _sc, table_name)
    sqlContext.dropTempTable(table_name)

def iterate_files_from_file(sc,ss, sqlContext, path):
    files = getFilePathsFromFile(sc, path)
    for file in files:
        extractMetaByColum(ss,sqlContext, file)

