'''
In PySpark Row class is available by importing pyspark.sql.Row
 which is represented as a record/row in DataFrame,
one can create a Row object by using named arguments, or create a custom Row like class.
Since Spark 3.0, Rows created from named arguments are not sorted alphabetically
instead they will be ordered in the position entered.
To enable sorting by names, set the environment variable PYSPARK_ROW_FIELD_SORTING_ENABLED to true.
Row class provides a way to create a struct-type column as well.
'''
####>>>>>>>>>>>>>>>>>>>Spark Session initiation <<<<<<<<<<<<<<<<<<<<<<<<<<<
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#####>>>>>>>>>>>>>>Using Row class on PySpark RDD<<<<<<<<<<<<<<<<<<<<<<<<<<<<
data = [Row(name="James,,Smith",lang=["Python","Scala","C++"],state="CA"),
    Row(name="Michael,Rose,",lang=["Spark","Oracle","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

#######>>>>>>>>>>>>>>Using Row class on PySpark DataFrame<<<<<<<<<<<<<<<<<<<
#######>>>>>>>>>>>>>>Using using the above data list to create DF<<<<<<<<<<<
df=spark.createDataFrame(data)
df.printSchema()
df.show()


