from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_json, to_json, col, window, avg, min, max, expr, count
from config import config




def create_spark_session(app_name):
    conf = SparkConf().setAppName(app_name)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
def process_kafka_data(spark,  kafka_topic, schema):
    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", config['bootstrap.servers'])\
        .option("kafka.security.protocol","SASL_SSL") \
        .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(config["sasl.username"], config['sasl.password'])) \
        .option("kafka.ssl.endpoint.identification.algorithm", "https")\
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("subscribe", kafka_topic)\
        .option("startingOffsets", "earliest")\
        .load()
    
    df = df.selectExpr("cast(value as string) as value")\
          .withColumn("value", from_json("value", schema)).select("value.*")

    
    return df

def preprocess_data(df):
    df = df.groupBy(window("timestamp", "1500 second", '1 second'))\
        .agg(
              avg("TP2").alias("rolling_mean_TP2"),\
              min("TP2").alias("rolling_min_TP2"),\
              max("TP2").alias("rolling_max_TP2"),\
              avg("TP3").alias("rolling_mean_TP3"),\
              min("TP3").alias("rolling_min_TP3"),\
              max("TP3").alias("rolling_max_TP3"),\
              avg("H1").alias("rolling_mean_H1"),\
              min("H1").alias("rolling_min_H1"),\
              max("H1").alias("rolling_max_H1"),\
              avg("DV_pressure").alias("rolling_mean_DV_pressure"),\
              min("DV_pressure").alias("rolling_min_DV_pressure"),\
              max("DV_pressure").alias("rolling_max_DV_pressure"),\
              avg("Reservoirs").alias("rolling_mean_Reservoirs"),\
              min("Reservoirs").alias("rolling_min_Reservoirs"),\
              max("Reservoirs").alias("rolling_max_Reservoirs"),\
              avg("Oil_temperature").alias("rolling_mean_Oil_temperature"),\
              min("Oil_temperature").alias("rolling_min_Oil_temperature"),\
              max("Oil_temperature").alias("rolling_max_Oil_temperature"),\
              avg("Flowmeter").alias("rolling_mean_Flowmeter_temperature"),\
              min("Flowmeter").alias("rolling_min_Flowmeter_temperature"),\
              max("Flowmeter").alias("rolling_max_Flowmeter_temperature"),\
              avg("Motor_current").alias("rolling_mean_Motor_current"),\
              min("Motor_current").alias("rolling_min_Motor_current"),\
              max("Motor_current").alias("rolling_max_Motor_current"),\
              count('*').alias("record_count")
        )

    df = df.withColumn("window_start", df.window.start)\
        .withColumn("window_end", df.window.end)\
        .filter(col("record_count") == 1500)
    
    return df

def create_kafka_write_stream(df, outputmode  , output_topic, checkpoint_location):
    df  =  df.selectExpr("to_json(struct(*)) as value").writeStream\
              .format("kafka")\
              .outputMode(outputmode)\
              .option("kafka.bootstrap.servers", config['bootstrap.servers'])\
              .option("kafka.security.protocol","SASL_SSL") \
              .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(config["sasl.username"], config['sasl.password']) ) \
              .option("kafka.ssl.endpoint.identification.algorithm", "https")\
              .option("kafka.sasl.mechanism", "PLAIN") \
              .option('topic', output_topic)\
              .option("checkpointLocation", checkpoint_location)


    return df