from pyspark.sql import SparkSession,DataFrame
import pyspark.sql.functions as spark_func
import pyspark.sql.types as spark_types


def transform_data(df:DataFrame,data_mapping:dict) -> DataFrame:
    df = df.filter(spark_func.col('id').isNotNull()).drop_duplicates()
    df = df.select(*(spark_func.col(key).cast(value).alias(key) for key,value in data_mapping.items()))
    return df


def write_data(df:DataFrame,write_format:str,credentials:dict,table_name:str,write_mode:str) -> str:
    df.write.format('snowflake').options(**credentials).option('dbtable',table_name).mode(write_mode).save()
    return f'[Info] Data is loaded Successfully'

def read_data(spark:SparkSession,options:dict,mode:str,file_format:str,file_path:str) -> DataFrame:
    df = spark.read.\
        options(**options).\
            option('mode',mode).\
                format(file_format).\
                    load(file_path)
    return df