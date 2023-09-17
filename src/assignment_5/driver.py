from PysparkAssignments.src.assignment_5.util import *

spark = spark_session()
product_df = create_df(spark)
product_df.show()
country_amt = pivot_amount(product_df)
country_amt.show()
unpivoted_df = unpivot_country(product_df)
unpivoted_df.show()