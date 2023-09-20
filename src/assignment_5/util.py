from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode


def spark_session():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark


def create_df(spark):
    product_schema = StructType([StructField("Product", StringType(), True),
                                 StructField("Amount", IntegerType(), True),
                                 StructField("Country", StringType(), True)])
    product_data = [("Banana", 1000, "USA"),
                    ("Carrots", 1500, "INDIA"),
                    ("Beans", 1600, "Sweden"),
                    ("Orange", 2000, "UK"),
                    ("Orange", 2000, "UAE"),
                    ("Banana", 400, "China"),
                    ("Carrots", 1200, "China")]
    product_df = spark.createDataFrame(data=product_data, schema=product_schema)
    return product_df

#Find total amount exported to each country of each product.

def pivot_amount(product_df):
    country_total_df = product_df.groupBy("Product").pivot("Country").sum("Amount")
    return country_total_df

#Perform unpivot function on output of question 2
def unpivot_country(product_df):
    unpivot_df = product_df.select("Product", explode("Country").alias("Countries"))
    return unpivot_df