import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import when
from pyspark.sql.functions import col
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Drop Duplicate Records')
### Initiate spark session
spark = SparkSession.builder.appName('Dataframe Create').getOrCreate()
#Creates a spark data frame called as raw_data.
#JSON with corrupted lines everything works
dataframe = spark.read.option("multiline","true").json('E:\ADF_VIDEOS\SOURCE_FILES\EMPLOYEES.json')
dataframe.show(10)
dataframe_dropdup = dataframe.dropDuplicates()
dataframe_dropdup.show(10)
##Explain Utility of Spark Dataframe
dataframe_dropdup.explain(True)