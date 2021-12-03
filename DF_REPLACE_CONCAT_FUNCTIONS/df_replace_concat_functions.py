from pyspark.sql.types import *
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Bhende Venkat').getOrCreate()
data2 = [("ASHNETAPP01","VASHNTAPDQT","ASVC1AUTOFAXSYSTEM_nonrep_cifs_dqa_rightfax","200GB","cifs","ASVC1AUTOFAXSYSTEM","","Prod","",""),
("ASHNETAPP01","VASHNTAPDQT","ASVAWSLETTERS_nonrep_cifs_letters","130GB","cifs","ASVAWSLETTERS","","Prod","",""),
("ASHNETAPP01","VASHNTAPRD","ASVSYMCLI_rep_cifs_test_convert","10GB","cifs","ASVSYMCLI","Storage health check replication","Prod","",""),
("ASHNETAPP01","VASHNTAPRD","ASVSYMCLI_nonrep_cifs_support_documentation","10GB","cifs","ASVSYMCLI","Storage array build documentation","Prod","",""),
("ASHNETAPP01","VASHNTAPRD","BANETWORKRHOOB_nonrep_nfs_ashlanixconflow","125GB","nfs","BANETWORKRHOOB","","Prod","","")
  ]
schema = StructType([ \
    StructField("netapp_array", StringType(), True), \
    StructField("vserver", StringType(), True), \
    StructField("volume_name", StringType(), True), \
    StructField("volume_size_gb", StringType(), True), \
    StructField("Protocol", StringType(), True), \
    StructField("ASV", StringType(), True), \
    StructField("Other_notes", StringType(), True), \
    StructField("Operational_Level", StringType(), True), \
    StructField("Creation_Date", StringType(), True), \
    StructField("Copies", StringType(), True) \
    ])

df = spark.createDataFrame(data=data2, schema=schema)

df3 = df.select(concat_ws('_', df.volume_name, df.vserver).alias("physical_name"),
                (df.netapp_array.substr(1, 3).alias("region")))
#df3.withColumn("app_asv",expr("regexp_replace(df.ASV,"\r|\n|\t", "")").alias("replaced_value"))
df3.show(truncate=False)
