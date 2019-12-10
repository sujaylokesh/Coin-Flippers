import time
import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

data=spark.read.csv('clean.csv',header=True)
data.createOrReplaceTempView("table")

spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/2278906) AS `Per Citizen` FROM table where Borough="QUEENS" and SUBSTRING(`Created Date`,0,4)=="2010"').show()
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/1455720) AS `Per Citizen` FROM table where Borough="BRONX" and SUBSTRING(`Created Date`,0,4)=="2010"').show()
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/1455720) AS `Per Citizen` FROM table where Borough="BROOKLYN" and SUBSTRING(`Created Date`,0,4)=="2010"').show()
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/1455720) AS `Per Citizen` FROM table where Borough="MANHATTAN" and SUBSTRING(`Created Date`,0,4)=="2010"').show()
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/1455720) AS `Per Citizen` FROM table where Borough="STATEN ISLAND" and SUBSTRING(`Created Date`,0,4)=="2010"').show()

BrooklynComp_q1=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="BROOKLYN" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
QueensComp_q1=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="QUEENS" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
ManhattanComp_q1=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="MANHATTAN" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
BronxComp_q1=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="BRONX" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
StatenComp_q1=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="STATEN ISLAND" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')

spark.sql('SELECT `Created Date`,`Closed Date`,DATEDIFF(`Closed Date`,`Created Date`) AS difference FROM table WHERE BOROUGH="BROOKLYN"').show()
>>> df.groupby('BOROUGH').avg().show()