import pyspark
from pyspark import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql.functions import *
import re
conf = pyspark.SparkConf()

sc = pyspark.SparkContext.getOrCreate(conf=conf)
spark = SQLContext(sc)

df_schema = StructType([StructField('to_be_extracted', StringType())])

test_list = [
    ['1183 Amsterdam'],
    ['06123 Ankara'],
    ['08067 Barcelona'],
    ['3030 Bern'],
    ['75116 Paris'],
    ['1149-014 Lisbon'],
    ['00-999 Warsaw'],
    ['00199 Rome'],
    ['HR-10 040 Zagreb']
]
schema = StructType([
    StructField("postal_code", StringType(), False),
    StructField("city", StringType(), False)
])
df = spark.createDataFrame(data=test_list, schema = df_schema)
df.show(truncate=False)

###with out UDF

regex = r'^(.*?)\s(\w*?)$'
df3=df.withColumn('postal_code',regexp_extract(col('to_be_extracted'), regex, 1)) \
    .withColumn('city', regexp_extract(col('to_be_extracted'), regex, 2))
df3.show(truncate=False)


###Create Dataframe function

def extract_in_python(content):
    regex = r'^(.*?)\s(\w*?)$'

    search_result = re.search(regex, content)

    if search_result:
        postal_code = search_result.group(1)
        city = search_result.group(2)
        return postal_code, city
    else:
        return None, None

extract_udf = udf(extract_in_python, schema)


#df.withColumn('extracted', extract_udf(col('to_be_extracted')))
df2=df \
    .withColumn('extracted', extract_udf(col('to_be_extracted'))) \
    .select(col('to_be_extracted'), col("extracted.*"))
df2.show(truncate=False)

'''
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

udf_myFunction = udf(myFunction, IntegerType()) # if the function returns an int
df = df.withColumn("message", udf_myFunction("_3")) #"_3" being the column name of the column you want to consider

'''