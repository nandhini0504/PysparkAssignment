from PysparkAssignments.src.assignment_4.util import *

spark = spark_session()

user_df = create_dataframe(spark)
user_df.show()

select_col = select_df(user_df)
select_col.show()

user_new_df = add_column(user_df)
user_new_df.show()

salarynew_df=salary_change(user_df)
salarynew_df.show()
