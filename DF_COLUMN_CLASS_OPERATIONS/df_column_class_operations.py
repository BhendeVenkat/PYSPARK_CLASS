import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import when
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Create')

### Initiate spark session

spark = SparkSession.builder.appName('abc').getOrCreate()
####Read EMployees CSV file first 10 records

data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')]
columns=["fname","lname","id","gender"]
df=spark.createDataFrame(data,columns)

df.select(expr(" fname ||','|| lname").alias("fullName")).show()

#asc, desc to sort ascending and descending order repsectively.
df.sort(df.fname.asc()).show()
df.sort(df.fname.desc()).show()

#cast() & astype() – Used to convert the data Type.
df.select(df.fname,df.id.cast("int")).printSchema()
#between() – Returns a Boolean expression when a column values in between lower and upper bound.
df.filter(df.id.between(100,300)).show()
##contains() – Checks if a DataFrame column value contains a a value specified in this function.

df.filter(df.fname.contains("Cruise")).show()

#startswith() & endswith()– Checks if the value of the DataFrame Column starts and ends with a String respectively

df.filter(df.fname.endswith("Cruise")).show()
####Multiple conditions
df.filter(df.fname.startswith("T")).filter(df.fname.endswith("Cruise")).show()

###isNull & isNotNull() – Checks if the DataFrame column has NULL or non NULL values.
df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()

## like() & rlike() – Similar to SQL LIKE expression
df.select(df.fname,df.lname,df.id).filter(df.fname.like("%om")).show()
df.select(df.fname,df.lname,df.id).filter(df.fname.like("To%")).show()


#when() & otherwise() – It is similar to SQL Case When, executes sequence of expressions
#until it matches the condition and returns a value when match.
df.select(df.fname,df.lname,df.gender,when(df.gender=="M","Male")
              .when(df.gender=="F","Female")
              .when(df.gender.isNull() ,"Not Mentioned")
              .otherwise(df.gender).alias("new_gender")
    ).show()

#substr() – Returns a Column after getting sub string from the Column
print('from 1st  position 2 charters')
df.select(df.fname,df.fname.substr(1,2).alias("substr")).show()# from 1st  position 2 charters
print('from 3rd position one charter ')
df.select(df.fname,df.fname.substr(3,1).alias("substr")).show() # from 3rd position one charter 
# substr() in reserve manner
df.select(df.fname,df.fname.substr(-4,2).alias("substr")).show() # from 4th chareter in reverse print 2 chars
df.select(df.fname,df.fname.substr(-3,2).alias("substr")).show() # from 3rd chareter in reverse print 2 chars
df.select(df.fname,df.fname.substr(-4,1).alias("substr")).show() # from 4th chareter in reverse print 2 chars
