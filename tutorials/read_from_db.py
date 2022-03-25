import findspark
findspark.init()


from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('READ_FROM_DB') \
            .getOrCreate()

read_from_db_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://localhost:3306/TestFromSpark") \
                        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "username") \
                        .option("user","root") \
                        .option("password","MySqlPassword2020!") \
                        .load()

read_from_db_df.show()