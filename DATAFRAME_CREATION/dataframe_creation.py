import pyspark
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col,struct,when
from pyspark.sql import SparkSession
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Create')

### Initiate spark session


spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
############Create a Dataframe using data and StructType
###StructField – Defines the metadata of the DataFrame column
####we can specify the structure using StructType and StructField classes

data = [("James", "", "Smith", "36636", "M", 3000),
       ("Michael", "Rose", "", "40288", "M", 4000),
       ("Robert", "", "Williams", "42114", "M", 4000),
       ("Maria", "Anne", "Jones", "39192", "F", 4000),
       ("Jen", "Mary", "Brown", "", "F", -1)
       ]

schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
    ])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
print(df.schema.fieldNames.contains("firstname"))
df.show(truncate=False)


############we can use struct function to redine columns and /or make sub list of data as shown below
######Using PySpark SQL function struct(), we can change the struct of the existing
######DataFrame and add a new StructType to it.
updatedDF = df.withColumn("OtherInfo",
    struct(col("id").alias("identifier"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000,"Low")
      .when(col("salary").cast(IntegerType()) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  )).drop("id","gender","salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)
print(updatedDF.schema.json())