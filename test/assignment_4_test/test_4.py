import unittest
from src.assignment_2.utils import SparkSession, unix_timestamp,from_unixtime,col,to_utc_timestamp,date_format,\
    trim,from_utc_timestamp,unix_timestamp,lit,transform_keys,expr,StringType,IntegerType,\
    DateType,StructType,StructField,MapType,select_nested_columns,add_columns,change_column,change_datatype,\
    rename_nested_column,distinct_value,drop_columns

data = [
    ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "03011998", "M", 3000),
    ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "10111998", "M", 20000),
    ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "02012000", "M", 3000)
]

schema = StructType([
    StructField("name", MapType(StringType(), StringType()), True),
    StructField("dob", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])
class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("Pyspark assignment 2").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    def test_create_dataframe(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        actual_df = df.collect()

        expected_data = [
            ({"middlename": "", "firstname": "James", "lastname": "Smith"}, "03011998", "M", 3000),
            ({"middlename": "Rose", "firstname": "Michael", "lastname": ""}, "10111998", "M", 20000),
            ({"middlename": "", "firstname": "Robert", "lastname": "Williams"}, "02012000", "M", 3000)
        ]
        expected_schema = StructType([
            StructField("name", MapType(StringType(), StringType()), True),
            StructField("dob", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", IntegerType(), True)
        ])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        expected_df =  df2.collect()
        self.assertEqual(actual_df, expected_df)

    def test_select_nested_columns(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df=select_nested_columns(df)
        expected_data= [("James", "Smith", 3000),
                        ("Michael","", 20000),
                        ("Robert", "Williams", 3000)]
        expected_schema= StructType([StructField("firstname", StringType(), True),StructField("lastname", StringType(), True),
                                StructField("salary", IntegerType(),True)])

        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        actual_df=  result_df.collect()
        expected_df =  df2.collect()
        self.assertEqual(actual_df, expected_df)

    def test_add_columns(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = add_columns(df)

        expected_data = [({"middlename": "", "firstname": "James", "lastname": "Smith"}, "03011998", "M", 3000, "Ind", "Civil", 28),
            ({"middlename": "Rose", "firstname": "Michael", "lastname": ""}, "10111998", "M", 20000, "Ind", "Civil", 28),
            ({"middlename": "", "firstname": "Robert", "lastname": "Williams"}, "02012000", "M", 3000, "Ind", "Civil",28)]
        expected_schema = StructType([
            StructField("name", MapType(StringType(), StringType()), True),
            StructField("dob", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("Country", StringType(), True),
            StructField("department", StringType(), True),
            StructField("age", IntegerType(), True)])
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        actual_data = result_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(actual_data, expected_data)
    def test_change_column(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df=change_column(df)
        actual_data=result_df.collect()

        expected_data = [
            ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "03011998", "M", 6000),
            ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "10111998", "M", 40000),
            ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "02012000", "M", 6000)]
        expected_schema = StructType([
            StructField("name", MapType(StringType(), StringType()), True),
            StructField("dob", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", IntegerType(), True)
        ])
        df2=self.spark.createDataFrame(data=expected_data,schema=expected_schema)
        expected_output=df2.collect()
        self.assertEqual(actual_data,expected_output)
    def test_change_datatype(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = change_datatype(df)
        actual_input=result_df.collect()
        expected_data = [
            ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "03011998", "M", "3000"),
            ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "10111998", "M", "20000"),
            ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "02012000", "M", "3000")]
        expected_schema = StructType([
            StructField("name", MapType(StringType(), StringType()), True),
            StructField("dob", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", StringType(), True)])
        df2=self.spark.createDataFrame(data=expected_data,schema=expected_schema)
        expected_df=df2.collect()
        self.assertEqual(actual_input,expected_df)
    def test_rename_nested_column(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = rename_nested_column(df)
        actual_input = result_df.collect()
        expected_data = [
            ({"firstposition": "James", "secondposition": "", "lastposition": "Smith"}, "03011998", "M", 3000),
            ({"firstposition": "Michael", "middleposition": "Rose", "lastposition": ""}, "10111998", "M", 20000),
            ({"firstposition": "Robert", "middleposition": "", "lastposition": "Williams"}, "02012000", "M", 3000)]
        expected_schema = StructType([
            StructField("name", MapType(StringType(), StringType()), True),
            StructField("dob", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", IntegerType(), True)])
        df2=self.spark.createDataFrame(data=actual_input,schema=expected_schema)
        expected_df=df2.collect()
        self.assertEqual(actual_input,expected_df)
    def test_drop_columns(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = drop_columns(df)
        result_df.show(truncate=False)
        actual_input = result_df.show()
        expected_data = [
            ({"middlename": "","firstname": "James", "lastname": "Smith"}, "M"),
            ({"middlename": "Rose","firstname": "Michael",  "lastname": ""}, "M"),
            ({"middlename": "","firstname": "Robert",  "lastname": "Williams"},"M")]
        expected_schema = StructType([
            StructField("name", MapType(StringType(), StringType()), True),
            StructField("gender", StringType(), True)])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        df2.show(truncate=False)
        expected_df = df2.show()
        self.assertEqual(result_df.show(),df2.show())

    def test_distinct_value(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = distinct_value(df)
        actual_input = result_df.collect()
        expected_data = [
           ('10111998', 20000),
            ('02012000',3000),
            ('03011998',3000)
           ]
        expected_schema = StructType([
            StructField("dob", StringType(), True),
            StructField("salary", IntegerType(), True)])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        expected_df = df2.collect()
        self.assertEqual(actual_input, expected_df)


if __name__ == '__main__':
    unittest.main()