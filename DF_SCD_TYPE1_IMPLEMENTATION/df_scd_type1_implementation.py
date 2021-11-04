import pyspark
from pyspark.sql import SparkSession
print('Modules Imported')
spark=SparkSession.builder.appName('spark_scd_type1').getOrCreate()
#Import source dataset
'''
emp_src = spark.read.format("jdbc") \
 .option(“url”, “jdbc:oracle:thin:scott/scott@//localhost:1522/oracle”) \
 .option(“dbtable”, “emp_spark”).option(“user”, “scott”) \
 .option(“password”, “scott”).option(“driver”, “oracle.jdbc.driver.OracleDriver”).load()
 '''
emp_src.show(10)
#import Target dataset, as of now target has zero rows
emp_tgt = spark.read.format(“jdbc”) \
 .option(“url”, “jdbc:oracle:thin:scott/scott@//localhost:1522/oracle”) \
 .option(“dbtable”, “emp_spark_scd1”).option(“user”, “scott”) \
 .option(“password”, “scott”).option(“driver”, “oracle.jdbc.driver.OracleDriver”).load()