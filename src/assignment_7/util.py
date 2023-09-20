from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import logging
logging.basicConfig(filename="C:\\Users\\NandhiniK\\PycharmProjects\\logs\\pysparkfirst.log", filemode="w")
log = logging.getLogger()
log.setLevel(logging.WARN)

#creating function to create a spark session object
def create_sparksession():
    spark = SparkSession.builder\
                  .master("local[1]")\
                  .appName("assignment 7")\
                  .getOrCreate()
    return spark

#creating a dataframe for product table
def product_dataframe(spark):
    product_schema=StructType([StructField(name='ProductName',dataType=StringType()),
                          StructField(name='Issue Date',dataType=StringType()),
                          StructField(name='Price',dataType=IntegerType()),
                          StructField(name='Brand',dataType=StringType()),
                          StructField(name='Country',dataType=StringType()),
                          StructField(name='Product_Number',dataType=IntegerType())
                          ])
    product_df = spark.read.format('csv').options(header=True).schema(product_schema).load("C:/Users/NandhiniK/PycharmProjects/Pysparkassign/PysparkAssignment/resource/product.csv")

    return product_df

#creating a function to convert  the issue date to timestamp format.
def issue_date_timestamp(product_df):
    df_timestamp_string = product_df.withColumn("Issue_Date_timsetampformat",F.from_unixtime(F.col("Issue Date")/1000,"yyyy-MM-dd'T'HH:mm:ssZZZZ"))
    return df_timestamp_string

#creating a function to convert  the issue date to date type.
def issue_date_datestamp(df_timestamp_string):
    df_date = df_timestamp_string.withColumn("Date",F.date_format(F.col("Issue_Date_timsetampformat"),"yyyy-MM-dd"))
    return df_date

#creating a function to remove starting space in Brand Column.
def remove_space(product_df):                                                   #removes leading and trailing spaces from the "Brand" column and stores the result in a new column
    df_remove_space = product_df.withColumn("Brand_new",F.trim(F.col("Brand")))
    return df_remove_space

#creating a function to replace null values with empty spaces Country Column.
def replace_null(product_df):
    df_replace_null = product_df.withColumn("Country_new",F.when(F.col("Country") == "null", '').otherwise(F.col("Country")))
    return df_replace_null

#creating a function to create a dataframe for transaction table
def transaction_dataframe(spark):
    transaction_schema=StructType([StructField(name='SourceId',dataType=IntegerType()),
                          StructField(name='TransactionNumber',dataType=IntegerType()),
                          StructField(name='Language',dataType=StringType()),
                          StructField(name='ModelNumber',dataType=IntegerType()),
                          StructField(name='StartTime',dataType=StringType()),
                          StructField(name='ProductNumber',dataType=IntegerType())
                          ])
    transaction_df = spark.read.format('csv').options(header=True).schema(transaction_schema).load("C:/Users/NandhiniK/PycharmProjects/Pysparkassign/PysparkAssignment/resource/product transaction.csv")
    return transaction_df

# #creating a function to covert column names from camel case to snake case.

#creating a function to add a new column as start_time_ms and convert the StartTime column values to milliseconds.
def millisecond(transaction_df):
    df_millisecond = transaction_df.withColumn("StartTime_timestamp",F.to_timestamp(F.col("StartTime")))\
                                   .withColumn("start_time_ms",F.unix_timestamp(F.col("StartTime_timestamp")))\
                                   .drop("StartTime_timestamp")
    return df_millisecond

#creating a function to join both the dataframe
def joindataframe(df_replace_null,df_millisecond):
    joined_df = df_replace_null.join(df_millisecond,df_replace_null.Product_Number == df_millisecond.ProductNumber,"left")\
                                .drop("Product_Number")
    return joined_df

#creating a function to filter the country with language as EN
def filter_country(joined_df):
    df_filter = joined_df.filter(F.col("Language") == "EN")
    return df_filter
