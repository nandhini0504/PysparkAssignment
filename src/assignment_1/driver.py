from PysparkAssignments.src.assignment_1.util import *
import sys
sys.path.append('C:/Users/NandhiniK/PycharmProjects/SparkRepo/src/assignment1_test.driver.py')

spark=Spark_Session()

df_user = user_data(spark)
df_user.show()

df_transaction=transaction_data(spark)
df_transaction.show()

df_join=joined_data(df_user,df_transaction)
df_join.show()

# Unique Cities where each product is sold
df_unique_locations = count_of_unique_location(df_join)
df_unique_locations.show()

## The products bought by each user
product_user = user_products(df_join)
product_user.show()

# Total amount of spending by each user on each product.
amount = total_spending(df_join)
amount.show()

split_email= split_column(df_join)
split_email.show()

filtered_userdetails = user_details(df_join)
filtered_userdetails.show()