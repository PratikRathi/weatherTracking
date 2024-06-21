from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import col, from_json
import logging
from cassandra_setup import cassandra_connection

def spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,'
                                            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,'
                                            'org.apache.commons:commons-pool2:2.11.1') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")

        return spark_conn
    except Exception as e:
        logging.info(f"Couldn't create the spark session due to exception {e}")

def connect_to_kafka(spark_conn):
    try:
        df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'weather_data') \
            .option('startingOffsets', 'earliest') \
            .load()

        return df
    except Exception as e:
        return logging.warning(f"kafka dataframe could not be created because: {e}")

def get_structured_df(df):
    schema = StructType([
        StructField('name', StringType(), False),
        StructField('region', StringType(), False),
        StructField('country', StringType(), False),
        StructField('lat', StringType(), False),
        StructField('lon', StringType(), False),
        StructField('datetime', StringType(), False),
        StructField('time', StringType(), False),
        StructField('temp_degree_celcius', StringType(), False),
        StructField('weather', StringType(), False),
        StructField('weather_icon', StringType(), False),
        StructField('wind_kph', StringType(), False),
        StructField('precip_mm', StringType(), False),
        StructField('forecast_date', StringType(), False),
        StructField('forecast_max_temp', StringType(), False),
        StructField('forecast_min_temp', StringType(), False),
        StructField('forecast_avg_temp', StringType(), False),
        StructField('forecast_max_wind', StringType(), False),
        StructField('forecast_rain', StringType(), False),
        StructField('forecast_weather', StringType(), False),
        StructField('forecast_weather_icon', StringType(), False)
    ])

    df = df.selectExpr('CAST(value AS STRING)')\
        .select(from_json(col('value'), schema).alias('data')).select('data.*')

    return df

if __name__=='__main__':
    spark_conn = spark_connection()
    cassandra_conn = cassandra_connection()

    if spark_conn and cassandra_conn:
        df = connect_to_kafka(spark_conn)
        structured_df = get_structured_df(df)

        streaming_query = structured_df.writeStream \
            .trigger(processingTime="10 seconds") \
            .format("org.apache.spark.sql.cassandra") \
            .option('checkpointLocation', 'tmp/checkpoint') \
            .option('keyspace', 'weather') \
            .option('table', 'daily_weather') \
            .outputMode('append') \
            .start()

        streaming_query.awaitTermination()