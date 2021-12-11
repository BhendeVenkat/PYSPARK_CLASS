import pyspark
from pyspark.sql import SparkSession
def optimize_table(path_to_table, schema_and_table_name, zorder_by_columns, vacuum_days=7):
    spark = SparkSession.builder.appName('Dataframe Create').getOrCreate()
    optimize_sql = f"OPTIMIZE {schema_and_table_name} ZORDER BY ({zorder_by_columns});"
    print (optimize_sql)
    #optimize_results = spark.sql(optimize_sql)
    vacuum_sql = f"VACUUM {schema_and_table_name} RETAIN {vacuum_days} DAYS;"
    print(vacuum_sql)
    #spark.sql(vacuum_sql)
    #return optimize_sql + "\n" + str(optimize_results.rdd.collect()[0][1]) + "\n" + vacuum_sql

path = ""
table_name = "sc_core.pr_incremental_load"
zorder_by_column_names = "PR_NUM, PR_LINE_NBR"
vacuum_days = 0

#Call the function
if __name__ == '__main__':
    test_results = optimize_table(path, table_name, zorder_by_column_names,vacuum_days)
    print(test_results)