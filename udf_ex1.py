import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.function import udf

spark = SparkSession.builder.appName("UDF_EX1").getOrCreate()

def convert_case(input_str):
    resStr = ""
    arr = input_str.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

cols = ['SeqNo', 'Name']
values = [("1", "john jones"),
            ("2", "tracey smith"),
            ("3", "amy sanders")]

df = spark.createDataFrame(values, cols)
df.show(truncate=False)