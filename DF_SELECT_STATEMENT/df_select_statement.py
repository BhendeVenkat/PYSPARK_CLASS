import pyspark
from pyspark.sql import SparkSession
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Select Statement')
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
'''

df.show(truncate=True)

####Select Single & Multiple Columns From PySpark
df.select("firstname","lastname").show()
df.select(df.firstname,df.lastname).show()
df.select(df["firstname"],df["lastname"]).show()

############Select all columns############

# Select All columns from List
df.select(*columns).show()
df.select([col for col in df.columns]).show()
df.select("*").show()

df.show()
'''