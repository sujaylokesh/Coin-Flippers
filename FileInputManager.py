import sys
import datetime
from operator import add
import statistics
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import json
import task2_M as task2
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType


def customMap(file_name):
    fields = file_name.split('.')
    return fields[0].replace('[', "").replace(']', "").replace("-", "_"), fields[1]

def getFilePathsFromFile(sc, path):
    data = sc.textFile(path)
    file_arrays = data.first().split(',')
    file_rdd = sc.parallelize(file_arrays)
    results = file_rdd.map(lambda x: customMap(x)).collect()
    return results