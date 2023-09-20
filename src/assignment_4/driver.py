from PysparkAssignment.src.assignment_4.util import *

spark = spark_session()

user_df = create_dataframe(spark)
user_df.show()

select_col_df = select_df(user_df)
select_col_df.show()

user_new_df = add_column(user_df)
user_new_df.show()

salarynew_df=salary_change(user_df)
salarynew_df.show()

age_df=add_column(user_df)   #add column
age_df.show()

salarynew_df=salary_change(user_df)
salarynew_df.show()

change_datatype_df=change_datatype(user_df)
change_datatype_df.show()

rename_column_df= rename_column(user_df)
rename_column_df.show()

name_filter_df= name_filter(user_df)
name_filter_df.show()

df_drop= drop_columns(age_df)
df_drop.show()

df_distinct=distinct_value(user_df)
df_distinct.show()