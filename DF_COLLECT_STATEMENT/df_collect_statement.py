import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Select Statement')
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#select() is a transformation that returns a new DataFrame and holds the columns that are selected whereas
# collect() is an action that returns the entire data set in an Array to the driver.
#Never use collect() especially for big data sets  becuase it returns data in array set to driver
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

dataCollect = deptDF.collect()

print(dataCollect)

dataCollect2 = deptDF.select("dept_name").collect()
print(dataCollect2)

for row in dataCollect:
    print(row['dept_name'] + "," +str(row['dept_id']))