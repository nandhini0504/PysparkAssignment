from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType,IntegerType
from pyspark.sql.functions import *

#creating a function to create spark Session
def Spark_Session():
    spark = SparkSession.builder.appName("Spark_assignment_2").getOrCreate()
    return spark

# Define the custom schema
custom_schema = StructType([
    StructField("RechargeID", StringType(), True),
    StructField("RechargeDate", IntegerType(), True),
    StructField("RemainingDays", IntegerType(), True),
    StructField("ValidityStatus", StringType(), True)
])








# Recharge details.
''' Use 'date_file.csv' file'''
# 1.Read the csv file with a custom schema by using the column names hint given in 'date_file.csv'.
# 2.Create a new column called “changed_Recharge_date” with 'Recharge date' column where the date values represented in yyyyMMdd(integer) change it into default date format (yyyy-MM-dd) with date DataType.
# 3.drop the ‘RechargeDate’ column.
# 4.Delete the duplicate rows.
# 5.Total Count of rows.
# 6.Add the “RemainingDays” column value to the date part of the column ”changed_Recharge_date” and remane this colummn as “expiryDate”.
# 7.Change the “over” value in 'ValidityStatus' column by a new value called “validity expired”.
# 8.Get the count of the column 'expiry_date' filtered based upon the date between "2020-05-12" and "2020-07-11".

#Reading a CSV file and Creating a dataframe.

# creating a function to create data frame for data_file.csv file

def recharge_details(spark):
    df_recharge = spark.read.csv("C:/Users/NandhiniK/PycharmProjects/PysparkRepo/PysparkAssignments/resource/date_file.csv", header="true", inferSchema="true")
    return df_recharge

# 2.Create a new column called “changed_Recharge_date” with 'Recharge date' column where the date values represented in yyyyMMdd(integer) change it into default date format (yyyy-MM-dd) with date DataType.
#Creating new column.

# Add a new column 'changed_Recharge_date' with the date format conversion
# Syntax:  date_format(column,format)
# Example: date_format(current_timestamp(),"yyyy MM dd").alias("date_format")
def create_new_column(df_recharge, name, datatype):
    df_changed_rechargedate = df_recharge.withColumn("changed_Recharge_date",date_format(df_recharge["RechargeDate"].cast("string"), "yyyy-MM-dd"))
    return df_changed_rechargedate

#Drop the ‘RechargeDate’ column.
def drop_column(df_recharge):
    df_drop_rechargedate= df_recharge.drop("RechargeDate")
    return df_drop_rechargedate

#Removing duplicate row values.
def remove_duplicates(df_recharge):                                    # Remove duplicate values from a specific column df = df.dropDuplicates(['RechargeID'])
    df_remove_duplicates = df_recharge.dropDuplicates()
    return df_remove_duplicates


# Calculate the total count of rows
def count_rows(df_recharge):
    total_count = df_recharge.count()
     return total_count

# Add the “RemainingDays” column value to the date part of the column ”changed_Recharge_date” and remane this colummn as “expiryDate”.
#Creation of expiryDate column.
def expiry_date(df_recharge, colName1, colName2):
    df_expirydate = df_recharge.withColumn("expiryDate",expr("date_add(changed_Recharge_date, RemainingDays)")).drop("RemainingDays")  # Drop the original "RemainingDays" column
     return df_expirydate
    #df = df.withColumn("expiryDate",(col("changed_Recharge_date").cast("date") + col("RemainingDays").cast("int"))).drop("RemainingDays")  # Drop the original "RemainingDays" column


#when(col("ValidityStatus") == "over", "validity expired") checks if the value in the 'ValidityStatus' column is "over" and replaces it with "validity expired" if true..otherwise(col("ValidityStatus")) keeps the original value in the 'ValidityStatus' column if it's not "over."


#Changing value in the validity column (str1=value of column which we need to change, str2=value we need to set).
def change_value(df, name, str1, str2):
    pass
    #return df

#Filter the column
def count_date_based(df, name):
    pass
    #return df

