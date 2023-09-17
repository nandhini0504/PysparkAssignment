from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#creating a function to create spark Session
def Spark_Session():
    spark = SparkSession.builder.appName("Spark_assignment_1").getOrCreate()
    return spark

# 1.Count of unique locations.
# Rename the resultant column as "Count_of_unique_location".
# 2.Find out the products bought by each user.
# Rename the resultant column as "products_bought".
# 3.Total spending done by each user on each product.
# Rename the resultant column as "total_spend".
# 4.Split the 'email_id' column into two columns based on '@' symbol.
# Have the split values in two different columns such as “name” and “extension”.
# 5.list the user details who have bought items in a price range 10000-50000 along with product details.


#Reading a CSV file and Creating a dataframe.

#creating a function to create data frame which has contents of user.csv file
def user_data(spark):
    df_user = spark.read.csv("C:/Users/NandhiniK/PycharmProjects/PysparkRepo/PysparkAssignments/resource/user.csv",header="true",inferSchema="true")
    return df_user

#creating a function to create data frame which has contents of transaction.csv file
def transaction_data(spark):
    df_transaction=spark.read.csv("C:/Users/NandhiniK/PycharmProjects/PysparkRepo/PysparkAssignments/resource/transaction.csv",header="true",inferSchema="true")
    return df_transaction

#Joining dataframes.

#creating a function which will join the transaction and user data frames using inner join
def joined_data(df_user,df_transaction):
    df_join = df_transaction.join(df_user, df_user.user_id == df_transaction.user_id,"inner")
    return df_join


#Function to count unique location and also Rename the resultant column as "Count_of_unique_location".

def count_of_unique_location(df_join):       #def count_of_unique_location(df, colName):
    df_unique_locations= df_join.groupBy("product_description","location").agg(countDistinct("location").alias("Count_of_unique_location"))
    return df_unique_locations

#Function to list the products bought by each user.
def user_products(df_join):                 #def user_products(df, colName1, colName2):
    product_user = df_join.groupBy('user_id').agg(collect_list("product_description").alias("products_bought"))      #collect_set This function collects all unique values from that column  within the group and store them in array or set.
    return product_user

#Function to get the total spending by each user.

def total_spending(df_join):  #def total_spending(df, colName1, colName2):
    amount = df_join.groupBy('user_id', 'product_description').agg(sum('price').alias("total_spend"))
    return amount


# Split the 'email_id' column into two columns
def split_column(df_user):
    df_split_email =df_user.withColumn("email_id", split(df_join["email_id"], "\\@")[0])
    split_email = df_split_email.withColumnRenamed("name", "extension")           # Have the split values in two different columns such as “name” and “extension”.
    return split_email

#list the user details who have bought items in a price range 10000-50000 along with product details.

# Using Filter the results for the price range 10,000 to 50,000

def user_details(df_join, price):
    filtered_userdetails = df_join.filter((df_join['price'] >= 10000) & (df_join['price'] <= 50000))
    return filtered_userdetails
