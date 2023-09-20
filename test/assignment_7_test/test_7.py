import unittest
from PysparkAssignment.src.assignment_7.util import *


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a SparkSession
        cls.spark = SparkSession.builder.master("local[1]").appName("Testing assignment1").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    #testing the function which will convert the time from unix epoch to timestamp format
    def test_issue_date_timestamp(self):
        data =[("Washing Machine","1648770933000",20000,"Samsung","India",1),
                           ("Refrigerator","1648770999000",35000,"LG","null",2),
              ]
        schema = StructType([StructField(name='ProductName', dataType=StringType()),
                                     StructField(name='Issue Date', dataType=StringType()),
                                     StructField(name='Price', dataType=IntegerType()),
                                     StructField(name='Brand', dataType=StringType()),
                                     StructField(name='Country', dataType=StringType()),
                                     StructField(name='Product_Number', dataType=IntegerType())
                            ])
        df = self.spark.createDataFrame(data=data,schema=schema)
        actual_df = issue_date_timestamp(df)
        expected_data=[("Washing Machine","1648770933000",20000,"Samsung","India",1,"2022-04-01T05:25:33GMT+05:30"),
                        ("Refrigerator","1648770999000",35000,"LG","null",2,"2022-04-01T05:26:39GMT+05:30")]
        expected_schema = StructType([StructField(name='ProductName', dataType=StringType()),
                             StructField(name='Issue Date', dataType=StringType()),
                             StructField(name='Price', dataType=IntegerType()),
                             StructField(name='Brand', dataType=StringType()),
                             StructField(name='Country', dataType=StringType()),
                             StructField(name='Product_Number', dataType=IntegerType()),
                            StructField(name='Issue_Date_timsetampformat', dataType=StringType())
                             ])
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_df.collect()),sorted(expected_df.collect()))

    def test_issue_date_datestamp(self):
        data = [("Washing Machine","1648770933000",20000,"Samsung","India",1,"2022-04-01T05:25:33GMT+05:30"),
                ("Refrigerator","1648770999000",35000, "LG","null",2,"2022-04-01T05:26:39GMT+05:30"),
                ]
        schema = StructType([StructField(name='ProductName', dataType=StringType()),
                             StructField(name='Issue Date', dataType=StringType()),
                             StructField(name='Price', dataType=IntegerType()),
                             StructField(name='Brand', dataType=StringType()),
                             StructField(name='Country', dataType=StringType()),
                             StructField(name='Product_Number', dataType=IntegerType()),
                             StructField(name='Issue_Date_timsetampformat', dataType=StringType())
                             ])
        df = self.spark.createDataFrame(data=data, schema=schema)
        actual_df = issue_date_datestamp(df)
        expected_data = [("Washing Machine","1648770933000",20000,"Samsung","India",1,"2022-04-01T05:25:33GMT+05:30","2022-04-01"),
            ("Refrigerator","1648770999000",35000, "LG","null",2,"2022-04-01T05:26:39GMT+05:30","2022-04-01")]
        expected_schema = StructType([StructField(name='ProductName', dataType=StringType()),
                                      StructField(name='Issue Date', dataType=StringType()),
                                      StructField(name='Price', dataType=IntegerType()),
                                      StructField(name='Brand', dataType=StringType()),
                                      StructField(name='Country', dataType=StringType()),
                                      StructField(name='Product_Number', dataType=IntegerType()),
                                      StructField(name='Issue_Date_timsetampformat', dataType=StringType()),
                                      StructField(name='Date', dataType=StringType())
                                      ])
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_df.collect()))

    #testing the function which will remove leading space from the brand column
    def test_remove_space(self):
        data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1),
                               ("Refrigerator","1648770999000",35000,"  LG","null",2)
                  ]
        schema = StructType([StructField(name='ProductName', dataType=StringType()),
                                         StructField(name='Issue Date', dataType=StringType()),
                                         StructField(name='Price', dataType=IntegerType()),
                                         StructField(name='Brand', dataType=StringType()),
                                         StructField(name='Country', dataType=StringType()),
                                         StructField(name='Product_Number', dataType=IntegerType())
                                ])
        df = self.spark.createDataFrame(data=data, schema=schema)
        actual_df = remove_space(df)
        expected_data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1,"Samsung"),
                            ("Refrigerator","1648770999000",35000,"  LG","null",2,"LG")]
        expected_schema = StructType([StructField(name='ProductName', dataType=StringType()),
                                 StructField(name='Issue Date', dataType=StringType()),
                                 StructField(name='Price', dataType=IntegerType()),
                                 StructField(name='Brand', dataType=StringType()),
                                 StructField(name='Country', dataType=StringType()),
                                 StructField(name='Product_Number', dataType=IntegerType()),
                                StructField(name='Brand_new', dataType=StringType()),
                                     ])
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_df.collect()),sorted(expected_df.collect()))

    # testing the function which will replace null values with empty values in country column
    def test_replace_null(self):
            data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1),
                    ("Refrigerator", "1648770999000", 35000, "  LG", "null", 2)
                    ]
            schema = StructType([StructField(name='ProductName', dataType=StringType()),
                                 StructField(name='Issue Date', dataType=StringType()),
                                 StructField(name='Price', dataType=IntegerType()),
                                 StructField(name='Brand', dataType=StringType()),
                                 StructField(name='Country', dataType=StringType()),
                                 StructField(name='Product_Number', dataType=IntegerType())
                                 ])
            df = self.spark.createDataFrame(data=data, schema=schema)
            actual_df = replace_null(df)
            actual_df.show()
            expected_data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1,"India"),
                             ("Refrigerator", "1648770999000", 35000, "  LG", "null", 2,"")]
            expected_schema = StructType([StructField(name='ProductName', dataType=StringType()),
                                  StructField(name='Issue Date', dataType=StringType()),
                                  StructField(name='Price', dataType=IntegerType()),
                                  StructField(name='Brand', dataType=StringType()),
                                  StructField(name='Country', dataType=StringType()),
                                  StructField(name='Product_Number', dataType=IntegerType()),
                                  StructField(name='Country_new', dataType=StringType())
                                  ])
            expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
            self.assertEqual(sorted(actual_df.collect()), sorted(expected_df.collect()))

    def test_millisecond(self):
        data=[(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000",1),
              (150439,234567,"US",345678,"2021-12-27T08:21:14.645+0000",2)
            ]
        schema = StructType([StructField(name='SourceId', dataType=IntegerType()),
                             StructField(name='TransactionNumber', dataType=IntegerType()),
                             StructField(name='Language', dataType=StringType()),
                             StructField(name='ModelNumber', dataType=IntegerType()),
                             StructField(name='StartTime', dataType=StringType()),
                             StructField(name='ProductNumber', dataType=IntegerType())
                             ])
        df_transaction=self.spark.createDataFrame(data=data,schema=schema)
        actual_df = millisecond(df_transaction)
        expected_data = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1,1640593229),
                (150439, 234567, "US", 345678, "2021-12-27T08:21:14.645+0000", 2,1640593274)
                ]
        expected_schema = StructType([StructField(name='SourceId', dataType=IntegerType()),
                             StructField(name='TransactionNumber', dataType=IntegerType()),
                             StructField(name='Language', dataType=StringType()),
                             StructField(name='ModelNumber', dataType=IntegerType()),
                             StructField(name='StartTime', dataType=StringType()),
                             StructField(name='ProductNumber', dataType=IntegerType()),
                             StructField(name='start_time_ms', dataType=LongType())
                             ])
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_df.collect()))

    def test_joindataframe(self):

        data2 = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1, 1640593229),
                         (150439, 234567, "US", 345678, "2021-12-27T08:21:14.645+0000", 2, 1640593274)
                         ]
        schema2 = StructType([StructField(name='SourceId', dataType=IntegerType()),
                                      StructField(name='TransactionNumber', dataType=IntegerType()),
                                      StructField(name='Language', dataType=StringType()),
                                      StructField(name='ModelNumber', dataType=IntegerType()),
                                      StructField(name='StartTime', dataType=StringType()),
                                      StructField(name='ProductNumber', dataType=IntegerType()),
                                      StructField(name='start_time_ms', dataType=LongType())
                                      ])
        df_millisecond = self.spark.createDataFrame(data=data2, schema=schema2)

        data1 = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1, "India"),
                         ("Refridgerator", "1648770999000", 35000, "  LG", "null", 2, "")]
        schema1 = StructType([StructField(name='ProductName', dataType=StringType()),
                                      StructField(name='Issue Date', dataType=StringType()),
                                      StructField(name='Price', dataType=IntegerType()),
                                      StructField(name='Brand', dataType=StringType()),
                                      StructField(name='Country', dataType=StringType()),
                                      StructField(name='Product_Number', dataType=IntegerType()),
                                      StructField(name='Country_new', dataType=StringType())
                                      ])
        df_replace_null = self.spark.createDataFrame(data=data1, schema=schema1)
        actual_df=joindataframe(df_replace_null,df_millisecond)
        expected_data = [("Washing Machine",1648770933000,20000,"Samsung","India","India",150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000",1,1640593229),
                 ("Refridgerator",1648770999000,35000,"  LG","null","",150439,234567,"US",345678,"2021-12-27T08:21:14.645+0000",2,1640593274)
                 ]
        expected_schema = StructType([StructField(name='ProductName', dataType=StringType()),
                                      StructField(name='Issue Date', dataType=StringType()),
                                      StructField(name='Price', dataType=IntegerType()),
                                      StructField(name='Brand', dataType=StringType()),
                                      StructField(name='Country', dataType=StringType()),
                                      StructField(name='Country_new', dataType=StringType()),
                                      StructField(name='SourceId', dataType=IntegerType()),
                                      StructField(name='TransactionNumber', dataType=IntegerType()),
                                      StructField(name='Language', dataType=StringType()),
                                      StructField(name='ModelNumber', dataType=IntegerType()),
                                      StructField(name='StartTime', dataType=StringType()),
                                      StructField(name='ProductNumber', dataType=IntegerType()),
                                      StructField(name='start_time_ms', dataType=LongType())
                              ])
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_df.collect()),sorted(expected_df.collect()))

    def test_filter_country(self):
        data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", "India", 150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1, 1640593229),
                         ("Refridgerator", 1648770999000, 35000, "  LG", "null", "", 150439, 234567, "US", 345678,"2021-12-27T08:21:14.645+0000", 2, 1640593274)
                         ]
        schema = StructType([StructField(name='ProductName', dataType=StringType()),
                                      StructField(name='Issue Date', dataType=StringType()),
                                      StructField(name='Price', dataType=IntegerType()),
                                      StructField(name='Brand', dataType=StringType()),
                                      StructField(name='Country', dataType=StringType()),
                                      StructField(name='Country_new', dataType=StringType()),
                                      StructField(name='SourceId', dataType=IntegerType()),
                                      StructField(name='TransactionNumber', dataType=IntegerType()),
                                      StructField(name='Language', dataType=StringType()),
                                      StructField(name='ModelNumber', dataType=IntegerType()),
                                      StructField(name='StartTime', dataType=StringType()),
                                      StructField(name='ProductNumber', dataType=IntegerType()),
                                      StructField(name='start_time_ms', dataType=LongType())
                                      ])
        df = self.spark.createDataFrame(data=data, schema=schema)
        actual_df=filter_country(df)
        expected_data = [("Washing Machine",1648770933000,20000,"Samsung","India","India",150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000",1,1640593229)
                        ]
        expected_schema = StructType([StructField(name='ProductName', dataType=StringType()),
                             StructField(name='Issue Date', dataType=StringType()),
                             StructField(name='Price', dataType=IntegerType()),
                             StructField(name='Brand', dataType=StringType()),
                             StructField(name='Country', dataType=StringType()),
                             StructField(name='Country_new', dataType=StringType()),
                             StructField(name='SourceId', dataType=IntegerType()),
                             StructField(name='TransactionNumber', dataType=IntegerType()),
                             StructField(name='Language', dataType=StringType()),
                             StructField(name='ModelNumber', dataType=IntegerType()),
                             StructField(name='StartTime', dataType=StringType()),
                             StructField(name='ProductNumber', dataType=IntegerType()),
                             StructField(name='start_time_ms', dataType=LongType())
                             ])
        expected_df=self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_df.collect()))




if __name__ == '__main__':
    unittest.main()