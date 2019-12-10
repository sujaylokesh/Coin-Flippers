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

output_path = ''
global final_results
global final_results2

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

def extractMetaByColum(_sc,spark, sqlContext, file_info, final_results2):
    file_path = '/user/hm74/NYCOpenData/'+ (file_info[0]).replace(" ","").replace("_","-")
    data = spark.read.csv(path=file_path, sep='\t', header=True, inferSchema=False)
    print("readdata")
    for col in range(0, len(data.columns)):
        data = data.withColumnRenamed(data.columns[col],
                                     Process_column_name_for_dataframe(data.columns[col]))
    print("column done")
    table_name = (file_info[1]).replace("-","_")
    column_name = file_info[2]

    data.createOrReplaceTempView(table_name)
    print("data view done")
    # p2.initialize()
    print("table name", table_name)
    data2 = p2.profile_colum(_sc, sqlContext, column_name,table_name)
    final_results2.append(data2)
    sqlContext.dropTempTable(table_name)

def iterate_files_from_file(sc,spark, sqlContext, path, out_path):
    global output_path
    output_path = out_path

    files = getFilePathsFromFile(sc, path)
    final_results2 =[]
    p2.initialize()
    print("initial done")
    for i in range(len(files)):
        try:
            extractMetaByColum(sc, spark, sqlContext, files[i], final_results2)
            if i % 1 == 0:
                path = "%s/Task2_%s.json" % (output_path, i)
                with open(path, 'w') as json_file:
                    json.dump(final_results2, json_file)
        except:
            print("error for file ", i, "skip it")
            e = sys.exc_info()
            print(e)

def getFilePathsFromFile_for_task1(sc, path):
    data = sc.textFile(path)
    file_arrays = data.first().split(',')
    for i in range(0, len(file_arrays)):
        file_arrays[i] = strip_char(file_arrays[i])
    return file_arrays

def iterate_files_from_file_for_task1(sc, ss, sqlContext, path, start_index, out_path):
    files = getFilePathsFromFile_for_task1(sc, path)
    global output_path
    output_path = out_path
    counter = 0
    final_results =[]
    for file in files:
        if counter < start_index:
            counter += 1
            continue
        file_path = output_path + (file).replace(" ","").replace("_","-")
        p1.extractMeta(ss, sqlContext, file_path, final_results)
        output_json_path = "%s/%s.json" % (output_path, counter)

        if counter % 1 == 0:
            with open(output_json_path, 'w') as json_file:
                json.dump(final_results, json_file)
                final_results.clear()

        counter += 1

def Process_column_name_for_dataframe(str):
    converted = []
    dict = {"#": "num", "%": "percent","@":"at", "&":"and",
            "*":"_","$":"dollar","+":"plus",
            "-":"_","=":"equal",
            "^":"6","!":"ex","(":"_",")":"_","\n":"_"
            }
    for i in range(0, len(str)):
        if str[i].isalnum():
            converted.append(str[i])
        elif str[i] in dict:
            converted.append(dict[str[i]])
        else:
            converted.append('_')
    result = ''.join(converted)
    return result
