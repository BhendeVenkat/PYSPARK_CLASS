import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import when
from pyspark.sql.functions import col
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Create')
### Initiate spark session
spark = SparkSession.builder.appName('abc').getOrCreate()
#Creates a spark data frame called as raw_data.
#JSON with corrupted lines everything works
dataframe = spark.read.option("multiline","true").json('E:\ADF_VIDEOS\SOURCE_FILES\EMPLOYEES.json')
dataframe.show(10)

#TXT FILES#
dataframe_txt = spark.read.option("multiline","true").text('E:\ADF_VIDEOS\SOURCE_FILES\Employees_Pyspark.txt')
dataframe.show(10)
#CSV FILES#
dataframe_csv = spark.read.option("multiline","true").csv('E:\ADF_VIDEOS\SOURCE_FILES\Employees_DF.csv')
dataframe.show(10)
#PARQUET FILES#
dataframe_parquet = spark.read.option("multiline","true").load('E:\ADF_VIDEOS\SOURCE_FILES\EMPLOYEES.parquet')
dataframe.show(10)