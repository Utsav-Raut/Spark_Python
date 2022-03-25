
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable
import shutil

# Clear any previous runs
shutil.rmtree("/home/boom/Documents/programming/delta_files/", ignore_errors=True)

spark = SparkSession \
        .builder.appName("MyApp").master('local[*]') \
                .getOrCreate()
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
          

from delta import *
# spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.range(0, 5)
data.write.format("delta").save("/home/boom/Documents/programming/delta_files/del1.delta")