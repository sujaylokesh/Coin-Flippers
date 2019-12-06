import sys
import datetime
from operator import add
import statistics
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
import task1_coinflippers as p1
import task2_coinflippers as p2

dumbo_path = '/user/hm74/NYCOpenData/'
local_mac_path ='/Users/mina/Downloads/testDumbo/'
local_windows_path ='E:\\homework\\big_data\\hw1\\project\\'
output_path = '/home/ml6543/project_final/output'

def strip_char(str):
    return str.replace('[', "")\
        .replace(']', "")\
        .replace("\\", "").replace("\'", "").replace("\`", "").replace("-", "_").replace(" ", "_")

def customMap(file_name):
    fields = file_name.split('.')
    table_name = strip_char(fields[0])
    col_name = strip_char(fields[1])
    name = strip_char(file_name.replace("." + fields[1], ""))

    return name, table_name, col_name

def getFilePathsFromFile(sc, path):
    data = sc.textFile(path)
    file_arrays = data.first().split(',')
    file_rdd = sc.parallelize(file_arrays)
    results = file_rdd.map(lambda x: customMap(x)).collect()
    return results

def extractMetaByColum(_sc,spark, sqlContext, file_info):
    file_path = local_windows_path + (file_info[0]).replace(" ","").replace("_","-")
    # file_path = '/user/hm74/NYCOpenData/'+ (file_info[0]).replace(" ","").replace("_","-")

    #file_path = (local_path + file_info[0]).replace(" ","").replace("_","-")

    data = spark.read.csv(path=file_path, sep='\t', header=True, inferSchema=False)
    for col in range(0, len(data.columns)):
        data = data.withColumnRenamed(data.columns[col],
                                     Process_column_name_for_dataframe(data.columns[col]))

    table_name = (file_info[1]).replace("-","_")
    column_name = file_info[2]
    data.createOrReplaceTempView(table_name)
    p2.initialize()
    data = p2.profile_colum(_sc, sqlContext, column_name,table_name)
    p2.output(_sc.parallelize(data), table_name)
    sqlContext.dropTempTable(table_name)

def iterate_files_from_file(sc,spark, sqlContext, path):
    files = getFilePathsFromFile(sc, path)
    for file in files:
        extractMetaByColum(sc, spark, sqlContext, file)


def getFilePathsFromFile_for_task1(sc, path):
    data = sc.textFile(path)
    file_arrays = data.first().split(',')
    for i in range(0, len(file_arrays)):
        file_arrays[i] = strip_char(file_arrays[i])
    return file_arrays

def iterate_files_from_file_for_task1(sc, ss, sqlContext, path, start_index):
    files = getFilePathsFromFile_for_task1(sc, path)
    counter = 0
    for file in files:
        if counter < start_index:
            counter += 1
            continue
        # file_path = dumbo_path + (file).replace(" ","").replace("_","-")
        file_path = local_windows_path + (file).replace(" ","").replace("_","-")
        p1.extractMeta(ss, sqlContext, file_path, counter)
        if counter % 2 == 0:
            f = open("%s/%s.txt" % (output_path, counter), "w")
            f.write(str(counter))
            f.close()
        counter += 1

def Process_column_name_for_dataframe(str):
    converted = []
    dict = {"#": "num", "%": "percent","@":"at", "&":"and",
            "*":"star","$":"dollar","+":"plus",
            "-":"minus","=":"equal",
            "^":"6","!":"ex","(":"open",")":"close"
            }
    for i in range(0, len(str)):
        if str[i].isalnum():
            converted.append(str[i])
        elif str[i] in dict:
            converted.append(dict[str[i]])
        else:
            converted.append('_')
    result = ''.join(converted)
    #print(result)
    return result
