from PysparkAssignment.src.assignment_6.util import *

spark = spark_session()

user_df = create_df(spark)
user_df.show()
dept_df = dept_group(user_df)
dept_df.show()
new_df = row_data(spark)
new_df.show()
highest_sal = highest_salary(user_df)
highest_sal.show()
multi_df = multi_action(user_df)
multi_df.show()