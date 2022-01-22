from pyspark.sql import SparkSession

spark_cluster = SparkSession \
    .builder \
    .appName("Data Loading") \
    .config('spark.jars', 'jars/snowflake-jdbc-3.13.14.jar,jars/spark-snowflake_2.12-2.9.2-spark_3.1.jar') \
    .getOrCreate()


##.config("spark.eventLog.enabled", "true") \