from PysparkAssignment.src.assignment_7.util import *


#calling a function to create a spark session object
spark = create_sparksession()


#calling a function to create a dataframe for product table
product_df = product_dataframe(spark)
print("Product Table")
product_df.show(truncate=False)

#calling a function to convert the issue date column to timestamp format
timestamp_df = issue_date_timestamp(product_df)
print("Product table with issue date as timestamp  format")
timestamp_df.show(truncate=False)

#calling a function to convert the issue date column from timestamp to datestamp format
datestamp_df=issue_date_datestamp(timestamp_df)
print("Product table with issue date as datestamp  format")
datestamp_df.show(truncate=False)

#calling a function to remove the leading spaces in Brand column
remove_brand_space_df = remove_space(product_df)
print("Product table with a new Brand column that has no leading space")
remove_brand_space_df.show(truncate=False)

#calling a function to replace null values with empty spaces
replace_null_df = replace_null(product_df)
print("Product table with a new Country column where the null values are replaced")
replace_null_df.show(truncate=False)

#calling a function to create a dataframe for transaction table
transaction_df=transaction_dataframe(spark)
print("Transaction table")
transaction_df.show(truncate=False)

#calling a function to convert StartTime to millisecond in a newcolumn start_time_ms
millisecond_df = millisecond(transaction_df)
print("Transaction table with new start_time_ms column")
millisecond_df.show(truncate=False)

#calling a function to join both the tables.
joined_df = joindataframe(replace_null_df,millisecond_df)
print("Joined Tables")

joined_df.show(truncate=False)

#calling a function to filter the country with language as EN.
filter_df = filter_country(joined_df)
print("Filtered dataframe with country having language as EN")
filter_df.show(truncate=False)