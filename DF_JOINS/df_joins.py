import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import when
from pyspark.sql.functions import col
import logging
logging.basicConfig(filename='Log4j.log',filemode='w',level=logging.DEBUG)
logger = logging.getLogger('Dataframe Joins')

### Initiate spark session

spark = SparkSession.builder.appName('Joins').getOrCreate()
emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40), \
    ("HR",60) \
  ]

deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
'''
empDF.printSchema()
empDF.show(truncate=False)
deptDF.printSchema()
empDF.show(truncate=False)
deptDF.show(truncate=False)

### Inner Join fetches only matching records
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)
#### FUll Outer Join returns all the matching records and aditional records from both side

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter") \
    .show(truncate=False)
##All matching + Left Side as SQL Left Outer Join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter") \
    .show(truncate=False)

##All matching + Right Side as SQL Left Outer Join

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
    .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"righttouter") \
    .show(truncate=False)
#this join returns columns from the only left dataset for the records match in the right dataset on join expression
# records not matched on join expression are ignored from both left and right datasets.
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi") \
   .show(truncate=False)
   
#leftanti join returns only columns from the left dataset for non-matched records.
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti") \
   .show(truncate=False)


#Self Join is nothing but joining dataframe with same dataframe


empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)
'''
#####Simple ANSI SQL Format#################


empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

joinDF = spark.sql("select * from EMP e left outer join DEPT d on e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

joinDF3 = spark.sql("select * from EMP e FULL outer JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

joinDF4 = spark.sql("select * from EMP e right outer  JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
  .show(truncate=False)
