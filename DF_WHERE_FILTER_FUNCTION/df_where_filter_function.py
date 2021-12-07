import pyspark
from pyspark.sql import SparkSession
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Select Statement')
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType
####PySpark filter() function is used to filter the rows from RDD/DataFrame based on the given condition
# or SQL expression, you can also use where() clause instead of the filter()
# if you are coming from an SQL background, both these functions operate exactly the same.



data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
'''
df.printSchema()

df.show(truncate=False)

df.filter(df.state == "OH").show()
df.filter((df.state == "OH") & (df.gender.like ("M%"))).show()

df.show(truncate=False)
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()
'''
df.show(truncate=False)
df.filter(df.name.lastname == "Williams") .show(truncate=False)