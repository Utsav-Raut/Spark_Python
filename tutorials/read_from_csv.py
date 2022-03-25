import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

try:

    file_path = "file:////home/boom/Documents/programming/pyspark/data_files/username.csv"

    spark = SparkSession.builder.master('local[*]').appName('READ_FROM_CSV').getOrCreate()
    file_df = spark.read.option("header", True).option("delimiter", ";").option("inferSchema", True) \
                    .csv(file_path).withColumn("FILE_NAME", input_file_name())

    file_df.createOrReplaceTempView('file_vw')
    cnt_file_df = file_df.count()
    print("Input file count of records = {}".format(str(cnt_file_df)))

    print("The columns in the table are:{}".format(file_df.columns))
    file_df.printSchema()
    # file_df.show()

except Exception as e:
    print('Exception occurred : '+str(e))