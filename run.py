from metadata import *
from credentails import *
from spark_cluster import *
from Transform import *
from spark_connector import *
import pyspark.sql.functions as F

def main() -> None:
    #spark_cluster._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark_cluster._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
    spark_cluster.sparkContext.setLogLevel("ERROR")
    users_data = read_data(spark=spark_cluster,options={"multiLine": True},mode='permissive',file_format='json',file_path=files['users'])
    transformed_users_data = transform_data(df=users_data,data_mapping=col_dtypes_users)
    transformed_users_data = transformed_users_data.withColumn('email', F.regexp_replace(F.col('email'), pattern='(?s).*@',replacement="@"))
    transformed_users_data = transformed_users_data.withColumn('address', F.md5(col='address'))
    transformed_users_data = transformed_users_data.withColumn('birthDate', F.md5(col='birthDate'))
    transformed_users_data = transformed_users_data.withColumn('firstname', F.md5(col='firstname'))
    transformed_users_data = transformed_users_data.withColumn('lastname', F.md5(col='lastname'))
    #transformed_users_data.show()
    write_data(df=transformed_users_data,write_format='snowflake',credentials=credential_properties,table_name='users',write_mode='overwrite')
    messages_data = read_data(spark=spark_cluster,options={"multiLine": True},mode='permissive',file_format='json',file_path=files['messages'])
    transformed_messages_data = transform_data(df=messages_data,data_mapping=col_dtypes_messages)
    transformed_messages_data = transformed_messages_data.withColumn('message', F.md5(col='message'))
    #transformed_messages_data.show()
    write_data(df=transformed_messages_data,write_format='snowflake',credentials=credential_properties,table_name='messages',write_mode='overwrite')
    connector()


if __name__ == '__main__':
    main()
