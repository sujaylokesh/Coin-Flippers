#module load python/gnu/3.6.5
#module load spark/2.4.0

# You need to import everything below
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, udf
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F

from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import SQLContext

import lime
from lime import lime_text
from lime.lime_text import LimeTextExplainer

import numpy as np
import pandas as pd

sc = SparkContext()
sql = SQLContext(sc)

spark = SparkSession \
    .builder \
    .appName("hw3") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

########################################################################################################
# Load data
categories = ["alt.atheism", "soc.religion.christian"]
LabeledDocument = pyspark.sql.Row("category", "text")


def categoryFromPath(path):
    return path.split("/")[-2]

# def prepareDF(typ):
#    rdds = [sc.wholeTextFiles("/user/tbl245/20news-bydate-" + typ + "/" + category)\
#             .map(lambda x: LabeledDocument(categoryFromPath(x[0]), x[1])) for category in categories]
#    return sc.union(rdds).toDF()


def prepareDF(typ):
    rdds =[sc.wholeTextFiles("/Users/mina/Dropbox/2019Fall/BigData/Bigdata_HW/hw3/hw3_data/20news-bydate-" + typ + "/" + category) \
                .map(lambda x: LabeledDocument(categoryFromPath(x[0]), x[1])) for category in categories]
    return sc.union(rdds).toDF()


train_df = prepareDF("train").cache()
test_df = prepareDF("test").cache()

#####################################################################################################
""" Task 1.1
a.	Compute the numbers of documents in training and test datasets. Make sure to write your code here and report
    the numbers in your txt files    

b.	Index each document in each dataset by creating an index column, "id", for each data set, with index starting at 0. 

"""
# Your code starts here
train_count = train_df.count()
test_count = test_df.count()
#test_df.show()

test_df = test_df.rdd.zipWithIndex().map(lambda x: (x[0][0], x[0][1], x[1])).toDF()  #flatmap?
test_df.createOrReplaceTempView("test")
test_df = sql.sql("select _1 as category, _2 as text, _3 as id from test")
test_df.show()

#test_df.rdd.collect()

#ab = test_df.select(format_string('%s\t%s, %s', test_df.category, test_df.text, test_df.id))#.write.save("report1", format="text")


#test_df.rdd.saveAsTextFile("report.txt")

########################################################################################################
# Build pipeline and run
indexer = StringIndexer(inputCol="category", outputCol="label")
tokenizer = RegexTokenizer(pattern=u'\W+', inputCol="text", outputCol="words", toLowercase=False)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(maxIter=20, regParam=0.001)

# Builing model pipeline
pipeline = Pipeline(stages=[indexer, tokenizer, hashingTF, idf, lr])

# Train model on training set
model = pipeline.fit(train_df)

# Model prediction on test set
pred = model.transform(test_df)

# Model prediction accuracy (F1-score)
pl = pred.select("label", "prediction").rdd.cache()
metrics = MulticlassMetrics(pl)
metrics.fMeasure()

#####################################################################################################
""" Task 1.2
a.	Run the model provided above. 
    Take your time to carefully understanding what is happening in this model pipeline.
    You are NOT allowed to make changes to this model's configurations.
    Compute and report the F1-score on the test dataset.
b.	Get and report the schema (column names and data types) of the model's prediction output.

"""
# Your code for this part, IF ANY, starts here
#pred.printSchema()

schema = []
for i in range(0, len(pred.schema.fields)):  # dataframe pred[schema[fields]]
    col = pred.schema.fields[i]
    name = col.name
    dtype = col.dataType
    row = pyspark.sql.Row(column_name = name, data_type= dtype)
    schema.append(row)

schema = sc.parallelize(schema).toDF()
print(schema)
#print(result)

#######################################################################################################
# Use LIME to explain example
#class_names = ['Atheism', 'Christian']
class_names = ['alt.atheism', 'soc.religion.christian']
explainer = LimeTextExplainer(class_names=class_names)

# Choose a random text in test set, change seed for randomness
#test_point = test_df.sample(False, 0.1, seed=10).limit(1)
#test_point.collect()
#test_point_label = test_point.select("category").collect()[0][0]

#test_point_text = test_point.select("text").collect()[0][0]


def classifier_fn(data):
    spark_object = spark.createDataFrame(data, "string").toDF("text")
    pred = model.transform(spark_object)
    output = np.array((pred.select("probability").collect())).reshape(len(data), 2)
    return output


# exp = explainer.explain_instance(test_point_text, classifier_fn, num_features=6)
# print('Probability(Christian) =', classifier_fn([test_point_text])[0][0])
# print('True class: %s' % class_names[categories.index(test_point_label)])
# exp.as_list()

#####################################################################################################
""" 
Task 1.3 : Output and report required details on test documents with ID’s 0, 275, and 664.
Task 1.4 : Generate explanations for all misclassified documents in the test set, sorted by conf in descending order, 
           and save this output (index, confidence, and LIME's explanation) to netID_misclassified_ordered.csv for submission.
"""
# Your code starts here

ids = [0,2]# 275, 664
missclassified = []
info =[]

misclassified_df = pred.filter(pred['label']!=pred['prediction'])
misclassified_id = [int(row['id']) for row in misclassified_df.collect()]

for i in range(0,len(misclassified_id)):
    temp =[]
    doc = pred.where("id == %s" % misclassified_id[i])
    text = doc.select("text").collect()[0][0]
    conf = abs(pred['probability'][0]-pred['probability'][0])
    explanations = explainer.explain_instance(text, classifier_fn, num_features=6)
    temp=[misclassified_id[i], conf, explanations.as_list()]
    info.append(temp)

df = pd.DataFrame(info, columns=['id','conf','explanations'])
print(df)
########################################################################################################
""" Task 1.5
Get the word and summation weight and frequency
"""




########################################################################################################
""" Task 2
Identify a feature-selection strategy to improve the model's F1-score.
Codes for your strategy is required
Retrain pipeline with your new train set (name it, new_train_df)
You are NOT allowed make changes to the test set
Give the new F1-score.
"""
#
# def myFunc(e):
#   return e[1][0]
#
# words_to_remove = words_confBig.sort(key=myFunc)


new_train_df = prepareDF("train").cache()
words_to_remove = ["edu", "com", "reply", "21"]

model = pipeline.fit(new_train_df)
pred = model.transform(test_df)
pl = pred.select("label", "prediction").rdd.cache()
metrics = MulticlassMetrics(pl)
metrics.fMeasure()

