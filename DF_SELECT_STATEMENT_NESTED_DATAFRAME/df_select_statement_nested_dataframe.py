import pyspark
from pyspark.sql import SparkSession
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Select Statement')
'''
In this article, you have learned select() is a transformation function of the DataFrame and is used to
 select single, multiple columns, select all columns from the list, 
select by index, and finally select nested struct columns, 
we have also learned how to select nested elements from the DataFrame
'''
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
'''
df2.printSchema()
####List all the columns this will not include (display )nested column names
df2.show(truncate=False) # shows all columns

###Only Name column this will not display nexted columns again
df2.select("name").show(truncate=False)

###Inorder to display nested column names we must need to specify inner column names
df2.select("name.firstname","name.lastname").show(truncate=False)
'''
df2.select ("name.firstname","name.middlename","name.lastname").show()