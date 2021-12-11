import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import when
from pyspark.sql.functions import col


MIN_SUM_THRESHOLD = 10, 000, 000


def has_columns(data_frame, column_name_list):
    if len(data_frame.columns) == 10:
        for column_name in column_name_list:
            if not data_frame.columns.contains(column_name):
                raise Exception('Column is missing: ' + column_name)
    else:
        raise Exception('Number Columns are mismatched')


'''
Avoid this to ensure everything goes in single function call
def column_count(data_frame):
    return data_frame.columns.size
'''

def process():
    # Create spark session
    spark = SparkSession.builder.getOrCreate()

    # very_large_dataframe
    # 250 GB of CSV files from client which must have only 10 columns [A, B, C, D, E, F, G, H, I, J]
    # [A, B] contains string data
    # [C, D, E, F, G, H, I, J] contains decimals with precision 5, scale 2 (i.e. 125.75)
    # [A, B, C, D, E] should not be null
    # [F, G, H, I, J] should may be null

    very_large_dataset_location = '/Sourced/location_1'
    very_large_dataframe = spark.read.csv(very_large_dataset_location, header=True, sep="\t")

    # validate column count
    #we can avoid this validation
    #if column_count(very_large_dataframe) != 10:
     #   raise Exception('Incorrect column count: ' + column_count(very_large_dataframe))

    # validate that dataframe has all required columns
    #this can we meta data driven apoproch
    has_columns(very_large_dataframe, ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'])

    # TODO
    # Column F: Need to apply conversion factor of 2.5 i.e. Value 2, conversion factor 2.5 = 5
    #very_large_dataframe = very_large_dataframe.withColumn("F", col("F") * 2.5) to override the column
    very_large_dataframe = very_large_dataframe.withColumn("Derived_F", col("F") * 2.5)
    # Remove duplicates in column [A]
    # 50% of records in column [A] could potentially be duplicates
    very_large_dataframe = very_large_dataframe.dropDuplicates(['A'])

    # Get count of column F and ensure it is above MIN_SUM_THRESHOLD
    total_sum_of_column_F = very_large_dataframe.agg(sum('F')).collect()[0][0] ### Need to replace collect()
    very_large_dataframe.createOrReplaceTempView("vld_inheap")#create temp dataframe
    total_sum_of_column_F= spark.sql("select sum(F)  from vld_inheap")#write an agg funtion on large DF
    #Get the first values of the dataframe usng head(1) ensure it is above MIN_SUM_THRESHOLD
    if total_sum_of_column_F.head(1) < MIN_SUM_THRESHOLD:
        raise Exception('total_sum_of_column_A: ' + total_sum_of_column_F.head(1) + ' is below threshold: ' + MIN_SUM_THRESHOLD)

    # small_geography_dimension_dataframe
    # 25 MB of parquet, 4 columns [A, K, L, M]
    # Columns [A, K, L] contain only string data
    # Column [M] is an integer
    # Columns [A, K, L, M] contain all non nullable data. Assume this is the case
    small_geography_dimension_dataset = '/location_2'
    small_geography_dimension_dataframe = spark.read.parquet(small_geography_dimension_dataset)

    # Join very_large_dataframe to small_geography_dimension_dataframe on column [A]
    # Include only column [M] from small_geography_dimension_dataframe on new very_large_dataframe
    # No data (row count) loss should occur from very_large_dataframe

    ###very_large_dataframe = very_large_dataframe.join(small_geography_dimension_dataframe, (very_large_dataframe.A == small_geography_dimension_dataframe.A))
    small_geography_dimension_dataframe.createOrReplaceTempView("sgf_inheap")  ## Create an heap temp rdbms table
    g_geographydimension = spark.sql("""select vld.*,sgf.M from  vld_inheap vld left join sgf_inheap sgf
        on sgf.A = vld.A """)  ## Multi line SQL Code Small table always should be on right side
    # small_product_dimension_dataframe
    # 50 MB of parquet, 4 columns [B, N, O, P]
    # Columns [B, N] contain only string data
    # Columns [O, P] contain only integers
    # Columns [B, N, O, P] contain all non nullable data. Assume this is the case
    small_product_dimension_dataset = './location_3'  # 50 MB of parquet
    small_product_dimension_dataframe = spark.read.parquet(small_product_dimension_dataset)

    # TODO
    # Join very_large_dataframe to small_product_dimension_dataframe on column [B]
    # Only join records to small_product_dimension_dataframe where O is greater then 10
    # Keep only Column [P]
    ## create an temp tempview on geo data frame
    g_geographydimension.createOrReplaceTempView("gdf_inheap")
    small_product_dimension_dataframe.createOrReplaceTempView("sdf_inheap")## Create an heap temp rdbms table
    p_productdimension=spark.sql("""select vld.C, vld.D, vld.E, vld.F, vld.G, vld.H, vld.I, vld.J, vld.M, sdf.P 
    from  gdf_inheap vld left join sdf_inheap sdf
    on    sdf.B = vld.B and sdf.O > 10"""  )## Multi line SQL Code Small table always should be on right side
    # Write very_large_dataframe to next stage.
    # Columns selected should be: [C, D, E, F, G, H, I, J, M, P]
    f_single_q_df=spark.sql("""select vld.C, vld.D, vld.E, vld.F, vld.G, vld.H, vld.I, vld.J,gdf.m,sdf.p
                    from vld_inheap vld left join gdf_inheap gdf on gdf.A = vld.A 
                    left join sdf_inheap sdf    on    sdf.B = vld.B and sdf.O > 10""") ## final single query dataframe

f_single_q_df.write.option("compression","snappy").parquet("./location_3", mode='overwrite')

'''  (f_single_q_df.write ##.coalesce(1)
     .mode('overwrite')
     .format('com.databricks.spark.csv')
     .option('header', 'true')
     .option('sep', '\t').csv('./location_3'))'''
## Need remove coalesce with
    # The next stage will involve presenting the data in Azure SQL Synpase for reporting by our team

    # Assume external table has been created in Azure SQL Synpase which point to location_3
    # This data is then loaded (Create Table AS - CTAS) into [assessment].[FactVeryLarge] for performance reasons

    # TODO
    # Sumarise benefits of the Create Table AS (CTAS) operation prior to giving access to reporting team.

    # The reporting team will join this newly created Fact (very_large_dataframe) back to Geography and Product Dimensions
    # for futher analysis


process()
