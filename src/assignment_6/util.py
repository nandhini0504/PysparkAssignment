from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *


def spark_session():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark


def create_df(spark):
    user_schema = StructType([StructField("employee_name", StringType(), True),
                                  StructField("department", StringType(), True),
                                  StructField("salary", IntegerType(), True)
                                  ])
    user_data = [("James", "Sales", 3000),
                     ("Michael", "Sales", 4600),
                     ("Robert", "Sales", 4100),
                     ("Maria", "Finance", 3000),
                     ("Raman", "Finance", 3000),
                     ("Scott", "Finance", 3300),
                     ("Jen", "Finance", 3900),
                     ("Jeff", "Marketing", 3000),
                     ("Kumar", "Marketing", 2000)]
    user_df = spark.createDataFrame(data=user_data, schema=user_schema)
    return user_df

# #creating a function to select first row from each department group.
def dept_group(user_df):
    partitioned_dept = Window.partitionBy("department").orderBy("employee_name")
    row_number_added = user_df.withColumn("row_number", row_number().over(partitioned_dept))
    first_row = row_number_added.filter(row_number_added.row_number == 1).drop("row_number")
    return first_row

#creating a function to retrieve employees who earns the highest salary.
def row_data(spark):
    schema_emp = StructType([StructField("name", StringType(), True),
                             StructField("age", IntegerType(), True),
                             StructField("Job", StringType(), True)
                             ])
    row = ("Anil", 25, "IT")
    employee_data = [("Guna", 26, "Admin"),
                     ("Mani", 27, "HR")]

    added_data = [row] + employee_data

    new_df = spark.createDataFrame(data=added_data, schema=schema_emp)
    return new_df

#creating a function to select the highest, lowest, average, and total salary for each department group.
def highest_salary(user_df):
    highest_salary = Window.partitionBy("department").orderBy(col("salary").desc())
    row_number_added = user_df.withColumn("row_number", row_number().over(highest_salary))
    filtered_df = row_number_added.filter(col("row_number") == 1).drop("row_number")
    return filtered_df


def multi_action(user_df):
    # highest salary
    high_salary = Window.partitionBy("department").orderBy(col("salary").desc())
    row_number_add1 = user_df.withColumn("row_number", row_number().over(high_salary))
    high_df = row_number_add1.filter(col("row_number") == 1).drop("row_number")

    # lowest salary
    low_salary = Window.partitionBy("department").orderBy(col("salary").asc())
    row_number_add2 = user_df.withColumn("row_number", row_number().over(low_salary))
    low_df = row_number_add2.filter(col("row_number") == 1).drop("row_number")
    low_df.show()
    # average salary
    avg_salary = user_df.groupBy("department").agg(avg("salary"))
    avg_salary.show()
    # total salary for each department
    tot_salary = user_df.groupBy("department").agg(sum("salary"))
    tot_salary.show()
    return high_df