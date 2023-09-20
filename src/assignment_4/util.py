import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


#Create SparkSession
def spark_session():
    spark=SparkSession.builder.appName("assignment4").getOrCreate()
    return spark

#Create dataframe
 #Create Schema
def create_dataframe(spark):

    user_data=  [
    (("James", "", "Smith"), "03011998", "M", 3000),
    (("Michael", "Rose", ""), "10111998", "M", 20000),
    (("Robert", "", "Williams"), "02012000", "M", 3000),
    (("Maria", "Anne", "Jones"), "03011998", "F", 11000),
    (("Jen", "Mary", "Brown"), "04101998", "F", 10000)
]

    user_schema=StructType([
        StructField('name',StructType([
    StructField("firstname",StringType(),True),
    StructField("middlename",StringType(),True),
    StructField("lastname",StringType(),True)
            ])),
    StructField("dob",StringType(),True),
    StructField("gender",StringType(),True),
    StructField("salary",IntegerType(),True)
    ])

    user_df=spark.createDataFrame(data=user_data,schema=user_schema)
    return user_df

#Select firstname, lastname and salary from Dataframe
def select_df(user_df):
    select_col_df=user_df.select(col("name.firstname"), col("name.lastname"), col("salary"))   # nested columns like "name.firstname" and "name.lastname use col
    return select_col_df

#Add Country, department, and age column in the dataframe
def add_column(user_df):
    country_df= user_df.withColumn("country",lit("India"))
    department_df=country_df.withColumn("department",lit("sales"))
    age_df=department_df.withColumn("age",
                when(col("name.firstname") == "James", lit("30")).
                when(col("name.firstname")=="Michael",lit("23")).
                when(col("name.firstname") == "Robert", lit("25")).
                when(col("name.firstname") == "Maria", lit("20")).
                otherwise(lit("20")))

    return age_df


#Change the value of salary column
def salary_change(user_df):
    salarynew_df=user_df.withColumn("salarynew",(col("salary")*2))
    return salarynew_df


#Change the data types of DOB and salary to String

def change_datatype(user_df):
    change_datatype_df= user_df.withColumn("dob",col("dob").cast(StringType())).withColumn("salary",col("salary").cast(StringType()))
    return change_datatype_df

#Derive new column from salary column.

#Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)

def rename_column(user_df):
    rename_column_df=user_df.select(col("name.firstname").alias("firstposition"),
                                   col("name.middlename").alias("secondposition"),
                                   col("name.lastname").alias("lastposition")
                                   )
    return rename_column_df

#Filter the name column whose salary in maximum.
def name_filter(user_df):                                       #Collect method collects the result of the aggregation operation. It returns a list of Row objects, where each Row corresponds to a row of the result
    max_salary = user_df.agg({"salary": "max"}).collect()[0][0]          #stores that maximum salary value in the variable max_salary
    name_filter_df = user_df.filter(user_df.salary == max_salary).select("name")
    return name_filter_df

#Drop the department and age column. [#df.drop(*cols) is used to drop the specified columns (in this case, "dob" and "salary") from the DataFrame df.
# The * operator is used to unpack the list cols into individual column names]

def drop_columns(df):
    column_drop = ["dob", "salary"]
    df_drop = df.drop(*column_drop)
    return df_drop

#List out distinct value of dob and salary
def distinct_value(df):
    df_distinct = df.select("dob", "salary").distinct()
    return df_distinct



