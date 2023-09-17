import unittest
from PysparkAssignments.src.assignment_5.utils import expr,StringType,IntegerType,DateType,StructType,\
    StructField,SparkSession,pivot_df, unpivot_dataframe

data = [
        ("banana", 1000, "USA"),
        ("carrots", 1500, "INDIA"),
        ("beans", 1600, "sweden"),
        ("orange", 2000, "UK"),
        ("orange", 2000, "UAE"),
        ("banana", 400, "CHINA"),
        ("carrots", 1200, "CHINA")
    ]
schema = StructType([
        StructField("product", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("country", StringType(), True)
    ])

class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("Pyspark assignment 5").getOrCreate()
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    def test_pivot_table(self):
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df=pivot_df(df)
        actual_df=result_df.collect()

        expected_data = [
            ("orange", None, None, 2000, 2000, None, None),
            ("beans", None, None, None, None, None, 1600),
            ("banana", 400, None, None, None, 1000, None),
            ("carrots", 1200, 1500, None, None, None, None)
        ]
        expected_schema = StructType([
            StructField("product", StringType(), True),
            StructField("CHINA", IntegerType(), True),
            StructField("INDIA", IntegerType(), True),
            StructField("UAE", IntegerType(), True),
            StructField("UK", IntegerType(), True),
            StructField("USA", IntegerType(), True),
            StructField("Sweden", IntegerType(), True)])
        df2 = self.spark.createDataFrame(data=expected_data,schema=expected_schema)
        expected_df=df2.collect()
        self.assertEqual(actual_df,expected_df)
    def test_unpivot_dataframe(self):
        data = [
            ("orange", None, None, 2000, 2000, None, None),
            ("beans", None, None, None, None, None, 1600),
            ("banana", 400, None, None, None, 1000, None),
            ("carrots", 1200, 1500, None, None, None, None)
        ]
        schema = StructType([
            StructField("product", StringType(), True),
            StructField("CHINA", IntegerType(), True),
            StructField("INDIA", IntegerType(), True),
            StructField("UAE", IntegerType(), True),
            StructField("UK", IntegerType(), True),
            StructField("USA", IntegerType(), True),
            StructField("Sweden", IntegerType(), True)])
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = unpivot_dataframe(df)
        actual_df = result_df.collect()
        expected_data = [
            ("orange", "uae", 2000),
            ("orange", "uk", 2000),
            ("beans", "sweden", 1600),
            ("banana", "china", 400),
            ("banana", "usa", 1000),
            ("carrots", "china", 1200),
            ("carrots", "india", 1500)
        ]
        expected_schema = StructType([
            StructField("product", StringType(), True),
            StructField("country", StringType(), True),
            StructField("Total", IntegerType(), True)
        ])
        df2=self.spark.createDataFrame(data=expected_data,schema=expected_schema)
        expected_df = df2.collect()
        self.assertEqual(actual_df,expected_df)
if __name__ == '__main__':
    unittest.main()