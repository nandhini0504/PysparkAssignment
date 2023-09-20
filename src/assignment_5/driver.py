from PysparkAssignment.src.assignment_5.util import *

spark = spark_session()

product_df = create_df(spark)
product_df.show(truncate=False)

country_amount = pivot_amount(product_df)
country_amount.show(truncate=False)

unpivot_df = unpivot_country(product_df)
unpivot_df.show(truncate=False)