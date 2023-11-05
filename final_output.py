from schema import org_schema,agg_schema
import argparse
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
from streaming import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--outputmode" , help="set outputmode")
    parser.add_argument("--checkpoint" , help="set checkpoint location")
    parser.add_argument("--input_topic_1" , help="set input topic 1")
    parser.add_argument("--input_topic_2" , help="set input topic 2")
    parser.add_argument("--output_topic" , help="set output topic")
    
    args = parser.parse_args()
    spark = create_spark_session('create final_output')
    raw_df = process_kafka_data(spark,  args.input_topic_1, org_schema)
    raw_df = raw_df.withColumn("timestamp", raw_df["timestamp"].cast(TimestampType()))
    # Process aggregation data
    agg_df = process_kafka_data(spark,  args.input_topic_2, agg_schema)
    # Select columns for lag DataFrame
    selected_columns = [col("TP2").alias("TP2_lag"), col("TP3").alias("TP3_lag"),
                        col("H1").alias("H1_lag"), col("DV_pressure").alias("DV_pressure_lag"),
                        col("Reservoirs").alias("Reservoirs_lag"), col("Oil_temperature").alias("Oil_temperature_lag"),
                        col("Flowmeter").alias("Flowmeter_lag"), col("Motor_current").alias("Motor_current_lag"),
                        col("timestamp").alias("timestamp_lag")]

    lag_df = raw_df.select("*").select(*selected_columns)
    # Join data
    join_condition_1 = agg_df['window_start'] == lag_df['timestamp_lag']
    result_df = agg_df.join(lag_df, join_condition_1)
    
    join_condition_2 = result_df['window_end'] == raw_df['timestamp']    
    result_df = result_df.join(raw_df, join_condition_2)

    # Drop unnecessary columns
    columns_to_drop = ['window_start', 'timestamp_lag', 'timestamp','record_count']
    result_df = result_df.drop(*columns_to_drop)
    result_df = result_df.withColumnRenamed("window_end", "timestamp")
    final_df = create_kafka_write_stream(result_df,args.outputmode , args.output_topic, args.checkpoint)
    final_df.start()\
            .awaitTermination()